package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgio"
)

const pgTimestampFormat = "2006-01-02 15:04:05.999999999"

// Timestamp represents the PostgreSQL timestamp type. The PostgreSQL
// timestamp does not have a time zone. This presents a problem when
// translating to and from time.Time which requires a time zone. It is highly
// recommended to use timestamptz whenever possible. Timestamp methods either
// convert to UTC or return an error on non-UTC times.
type Timestamp struct {
	Time             time.Time // Time must always be in UTC.
	Valid            bool
	InfinityModifier InfinityModifier
}

// Set converts src into a Timestamp and stores in dst. If src is a
// time.Time in a non-UTC time zone, the time zone is discarded.
func (dst *Timestamp) Set(src interface{}) error {
	if src == nil {
		*dst = Timestamp{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case time.Time:
		*dst = Timestamp{Time: time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), time.UTC), Valid: true}
	case *time.Time:
		if value == nil {
			*dst = Timestamp{}
		} else {
			return dst.Set(*value)
		}
	case InfinityModifier:
		*dst = Timestamp{InfinityModifier: value, Valid: true}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Timestamp", value)
	}

	return nil
}

func (dst Timestamp) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	if dst.InfinityModifier != None {
		return dst.InfinityModifier
	}
	return dst.Time
}

func (src *Timestamp) AssignTo(dst interface{}) error {
	if !src.Valid {
		return NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *time.Time:
		if src.InfinityModifier != None {
			return fmt.Errorf("cannot assign %v to %T", src, dst)
		}
		*v = src.Time
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}
}

// DecodeText decodes from src into dst. The decoded time is considered to
// be in UTC.
func (dst *Timestamp) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Timestamp{}
		return nil
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		*dst = Timestamp{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Timestamp{Valid: true, InfinityModifier: -Infinity}
	default:
		tim, err := time.Parse(pgTimestampFormat, sbuf)
		if err != nil {
			return err
		}

		*dst = Timestamp{Time: tim, Valid: true}
	}

	return nil
}

// DecodeBinary decodes from src into dst. The decoded time is considered to
// be in UTC.
func (dst *Timestamp) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Timestamp{}
		return nil
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for timestamp: %v", len(src))
	}

	microsecSinceY2K := int64(binary.BigEndian.Uint64(src))

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		*dst = Timestamp{Valid: true, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		*dst = Timestamp{Valid: true, InfinityModifier: -Infinity}
	default:
		tim := time.Unix(
			microsecFromUnixEpochToY2K/1000000+microsecSinceY2K/1000000,
			(microsecFromUnixEpochToY2K%1000000*1000)+(microsecSinceY2K%1000000*1000),
		).UTC()
		*dst = Timestamp{Time: tim, Valid: true}
	}

	return nil
}

// EncodeText writes the text encoding of src into w. If src.Time is not in
// the UTC time zone it returns an error.
func (src Timestamp) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}
	if src.Time.Location() != time.UTC {
		return nil, fmt.Errorf("cannot encode non-UTC time into timestamp")
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.Truncate(time.Microsecond).Format(pgTimestampFormat)
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return append(buf, s...), nil
}

// EncodeBinary writes the binary encoding of src into w. If src.Time is not in
// the UTC time zone it returns an error.
func (src Timestamp) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}
	if src.Time.Location() != time.UTC {
		return nil, fmt.Errorf("cannot encode non-UTC time into timestamp")
	}

	var microsecSinceY2K int64
	switch src.InfinityModifier {
	case None:
		microsecSinceUnixEpoch := src.Time.Unix()*1000000 + int64(src.Time.Nanosecond())/1000
		microsecSinceY2K = microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
	case Infinity:
		microsecSinceY2K = infinityMicrosecondOffset
	case NegativeInfinity:
		microsecSinceY2K = negativeInfinityMicrosecondOffset
	}

	return pgio.AppendInt64(buf, microsecSinceY2K), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Timestamp) Scan(src interface{}) error {
	if src == nil {
		*dst = Timestamp{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	case time.Time:
		*dst = Timestamp{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Timestamp) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	if src.InfinityModifier != None {
		return src.InfinityModifier.String(), nil
	}
	return src.Time, nil
}
