package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/jackc/pgx/pgio"
)

type Date struct {
	Time   time.Time
	Status Status
	InfinityModifier
}

const (
	negativeInfinityDayOffset = -2147483648
	infinityDayOffset         = 2147483647
)

func (dst *Date) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Date:
		*dst = value
	case time.Time:
		*dst = Date{Time: value, Status: Present}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Date", value)
	}

	return nil
}

func (src *Date) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *time.Time:
		if src.Status != Present || src.InfinityModifier != None {
			return fmt.Errorf("cannot assign %v to %T", src, dst)
		}
		*v = src.Time
	default:
		if v := reflect.ValueOf(dst); v.Kind() == reflect.Ptr {
			el := v.Elem()
			switch el.Kind() {
			// if dst is a pointer to pointer, strip the pointer and try again
			case reflect.Ptr:
				if src.Status == Null {
					el.Set(reflect.Zero(el.Type()))
					return nil
				}
				if el.IsNil() {
					// allocate destination
					el.Set(reflect.New(el.Type().Elem()))
				}
				return src.AssignTo(el.Interface())
			}
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *Date) DecodeText(src []byte) error {
	if src == nil {
		*dst = Date{Status: Null}
		return nil
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		*dst = Date{Status: Present, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Date{Status: Present, InfinityModifier: -Infinity}
	default:
		t, err := time.ParseInLocation("2006-01-02", sbuf, time.UTC)
		if err != nil {
			return err
		}

		*dst = Date{Time: t, Status: Present}
	}

	return nil
}

func (dst *Date) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Date{Status: Null}
		return nil
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length for date: %v", len(src))
	}

	dayOffset := int32(binary.BigEndian.Uint32(src))

	switch dayOffset {
	case infinityDayOffset:
		*dst = Date{Status: Present, InfinityModifier: Infinity}
	case negativeInfinityDayOffset:
		*dst = Date{Status: Present, InfinityModifier: -Infinity}
	default:
		t := time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.UTC)
		*dst = Date{Time: t, Status: Present}
	}

	return nil
}

func (src Date) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
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

	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}

	_, err = w.Write([]byte(s))
	return err
}

func (src Date) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
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

	_, err = pgio.WriteInt32(w, daysSinceDateEpoch)
	return err
}
