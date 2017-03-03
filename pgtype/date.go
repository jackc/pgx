package pgtype

import (
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

func (d *Date) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Date:
		*d = value
	case time.Time:
		*d = Date{Time: value, Status: Present}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return d.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Date", value)
	}

	return nil
}

func (d *Date) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *time.Time:
		if d.Status != Present {
			return fmt.Errorf("cannot assign %v to %T", d, dst)
		}
		*v = d.Time
	default:
		if v := reflect.ValueOf(dst); v.Kind() == reflect.Ptr {
			el := v.Elem()
			switch el.Kind() {
			// if dst is a pointer to pointer, strip the pointer and try again
			case reflect.Ptr:
				if d.Status == Null {
					if !el.IsNil() {
						// if the destination pointer is not nil, nil it out
						el.Set(reflect.Zero(el.Type()))
					}
					return nil
				}
				if el.IsNil() {
					// allocate destination
					el.Set(reflect.New(el.Type().Elem()))
				}
				return d.AssignTo(el.Interface())
			}
		}
		return fmt.Errorf("cannot decode %v into %T", d, dst)
	}

	return nil
}

func (d *Date) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*d = Date{Status: Null}
		return nil
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	sbuf := string(buf)
	switch sbuf {
	case "infinity":
		*d = Date{Status: Present, InfinityModifier: Infinity}
	case "-infinity":
		*d = Date{Status: Present, InfinityModifier: -Infinity}
	default:
		t, err := time.ParseInLocation("2006-01-02", sbuf, time.UTC)
		if err != nil {
			return err
		}

		*d = Date{Time: t, Status: Present}
	}

	return nil
}

func (d *Date) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*d = Date{Status: Null}
		return nil
	}

	if size != 4 {
		return fmt.Errorf("invalid length for date: %v", size)
	}

	dayOffset, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	switch dayOffset {
	case infinityDayOffset:
		*d = Date{Status: Present, InfinityModifier: Infinity}
	case negativeInfinityDayOffset:
		*d = Date{Status: Present, InfinityModifier: -Infinity}
	default:
		t := time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.UTC)
		*d = Date{Time: t, Status: Present}
	}

	return nil
}

func (d Date) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, d.Status); done {
		return err
	}

	var s string

	switch d.InfinityModifier {
	case None:
		s = d.Time.Format("2006-01-02")
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

func (d Date) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, d.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	var daysSinceDateEpoch int32
	switch d.InfinityModifier {
	case None:
		tUnix := time.Date(d.Time.Year(), d.Time.Month(), d.Time.Day(), 0, 0, 0, 0, time.UTC).Unix()
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
