package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgio"
)

const pgTimestamptzHourFormat = "2006-01-02 15:04:05.999999999Z07"
const pgTimestamptzMinuteFormat = "2006-01-02 15:04:05.999999999Z07:00"
const pgTimestamptzSecondFormat = "2006-01-02 15:04:05.999999999Z07:00:00"
const microsecFromUnixEpochToY2K = 946684800 * 1000000

const (
	negativeInfinityMicrosecondOffset = -9223372036854775808
	infinityMicrosecondOffset         = 9223372036854775807
)

type Timestamptz struct {
	Time             time.Time
	Valid            bool
	InfinityModifier InfinityModifier
}

func (dst *Timestamptz) Set(src interface{}) error {
	if src == nil {
		*dst = Timestamptz{}
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
		*dst = Timestamptz{Time: value, Valid: true}
	case *time.Time:
		if value == nil {
			*dst = Timestamptz{}
		} else {
			return dst.Set(*value)
		}
	case InfinityModifier:
		*dst = Timestamptz{InfinityModifier: value, Valid: true}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Timestamptz", value)
	}

	return nil
}

func (dst Timestamptz) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	if dst.InfinityModifier != None {
		return dst.InfinityModifier
	}
	return dst.Time
}

func (src *Timestamptz) AssignTo(dst interface{}) error {
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

func (dst *Timestamptz) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Timestamptz{}
		return nil
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		*dst = Timestamptz{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Timestamptz{Valid: true, InfinityModifier: -Infinity}
	default:
		var format string
		if len(sbuf) >= 9 && (sbuf[len(sbuf)-9] == '-' || sbuf[len(sbuf)-9] == '+') {
			format = pgTimestamptzSecondFormat
		} else if len(sbuf) >= 6 && (sbuf[len(sbuf)-6] == '-' || sbuf[len(sbuf)-6] == '+') {
			format = pgTimestamptzMinuteFormat
		} else {
			format = pgTimestamptzHourFormat
		}

		tim, err := time.Parse(format, sbuf)
		if err != nil {
			return err
		}

		*dst = Timestamptz{Time: tim, Valid: true}
	}

	return nil
}

func (dst *Timestamptz) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Timestamptz{}
		return nil
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for timestamptz: %v", len(src))
	}

	microsecSinceY2K := int64(binary.BigEndian.Uint64(src))

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		*dst = Timestamptz{Valid: true, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		*dst = Timestamptz{Valid: true, InfinityModifier: -Infinity}
	default:
		tim := time.Unix(
			microsecFromUnixEpochToY2K/1000000+microsecSinceY2K/1000000,
			(microsecFromUnixEpochToY2K%1000000*1000)+(microsecSinceY2K%1000000*1000),
		)
		*dst = Timestamptz{Time: tim, Valid: true}
	}

	return nil
}

func (src Timestamptz) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.UTC().Truncate(time.Microsecond).Format(pgTimestamptzSecondFormat)
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return append(buf, s...), nil
}

func (src Timestamptz) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
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
func (dst *Timestamptz) Scan(src interface{}) error {
	if src == nil {
		*dst = Timestamptz{}
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
		*dst = Timestamptz{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Timestamptz) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	if src.InfinityModifier != None {
		return src.InfinityModifier.String(), nil
	}
	return src.Time, nil
}

func (src Timestamptz) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.Format(time.RFC3339Nano)
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return json.Marshal(s)
}

func (dst *Timestamptz) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*dst = Timestamptz{}
		return nil
	}

	switch *s {
	case "infinity":
		*dst = Timestamptz{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Timestamptz{Valid: true, InfinityModifier: -Infinity}
	default:
		// PostgreSQL uses ISO 8601 for to_json function and casting from a string to timestamptz
		tim, err := time.Parse(time.RFC3339Nano, *s)
		if err != nil {
			return err
		}

		*dst = Timestamptz{Time: tim, Valid: true}
	}

	return nil
}
