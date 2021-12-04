package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgio"
)

type Date struct {
	Time             time.Time
	Valid            bool
	InfinityModifier InfinityModifier
}

const (
	negativeInfinityDayOffset = -2147483648
	infinityDayOffset         = 2147483647
)

func (dst *Date) Set(src interface{}) error {
	if src == nil {
		*dst = Date{}
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
		*dst = Date{Time: value, Valid: true}
	case string:
		return dst.DecodeText(nil, []byte(value))
	case *time.Time:
		if value == nil {
			*dst = Date{}
		} else {
			return dst.Set(*value)
		}
	case *string:
		if value == nil {
			*dst = Date{}
		} else {
			return dst.Set(*value)
		}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Date", value)
	}

	return nil
}

func (dst Date) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	if dst.InfinityModifier != None {
		return dst.InfinityModifier
	}
	return dst.Time
}

func (src *Date) AssignTo(dst interface{}) error {
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

func (dst *Date) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Date{}
		return nil
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		*dst = Date{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Date{Valid: true, InfinityModifier: -Infinity}
	default:
		t, err := time.ParseInLocation("2006-01-02", sbuf, time.UTC)
		if err != nil {
			return err
		}

		*dst = Date{Time: t, Valid: true}
	}

	return nil
}

func (dst *Date) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Date{}
		return nil
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length for date: %v", len(src))
	}

	dayOffset := int32(binary.BigEndian.Uint32(src))

	switch dayOffset {
	case infinityDayOffset:
		*dst = Date{Valid: true, InfinityModifier: Infinity}
	case negativeInfinityDayOffset:
		*dst = Date{Valid: true, InfinityModifier: -Infinity}
	default:
		t := time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.UTC)
		*dst = Date{Time: t, Valid: true}
	}

	return nil
}

func (src Date) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.Format("2006-01-02")
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return append(buf, s...), nil
}

func (src Date) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	var daysSinceDateEpoch int32
	switch src.InfinityModifier {
	case None:
		tUnix := time.Date(src.Time.Year(), src.Time.Month(), src.Time.Day(), 0, 0, 0, 0, time.UTC).Unix()
		dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

		secSinceDateEpoch := tUnix - dateEpoch
		daysSinceDateEpoch = int32(secSinceDateEpoch / 86400)
	case Infinity:
		daysSinceDateEpoch = infinityDayOffset
	case NegativeInfinity:
		daysSinceDateEpoch = negativeInfinityDayOffset
	}

	return pgio.AppendInt32(buf, daysSinceDateEpoch), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Date) Scan(src interface{}) error {
	if src == nil {
		*dst = Date{}
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
		*dst = Date{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Date) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	if src.InfinityModifier != None {
		return src.InfinityModifier.String(), nil
	}
	return src.Time, nil
}

func (src Date) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.Format("2006-01-02")
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return json.Marshal(s)
}

func (dst *Date) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*dst = Date{}
		return nil
	}

	switch *s {
	case "infinity":
		*dst = Date{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Date{Valid: true, InfinityModifier: -Infinity}
	default:
		t, err := time.ParseInLocation("2006-01-02", *s, time.UTC)
		if err != nil {
			return err
		}

		*dst = Date{Time: t, Valid: true}
	}

	return nil
}
