package pgtype

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/jackc/pgx/pgio"
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
	Time   time.Time
	Status Status
	InfinityModifier
}

func (t *Timestamptz) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Timestamptz:
		*t = value
	case time.Time:
		*t = Timestamptz{Time: value, Status: Present}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return t.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Timestamptz", value)
	}

	return nil
}

func (t *Timestamptz) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *time.Time:
		if t.Status != Present || t.InfinityModifier != None {
			return fmt.Errorf("cannot assign %v to %T", t, dst)
		}
		*v = t.Time
	default:
		if v := reflect.ValueOf(dst); v.Kind() == reflect.Ptr {
			el := v.Elem()
			switch el.Kind() {
			// if dst is a pointer to pointer, strip the pointer and try again
			case reflect.Ptr:
				if t.Status == Null {
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
				return t.AssignTo(el.Interface())
			}
		}
		return fmt.Errorf("cannot assign %v into %T", t, dst)
	}

	return nil
}

func (t *Timestamptz) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*t = Timestamptz{Status: Null}
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
		*t = Timestamptz{Status: Present, InfinityModifier: Infinity}
	case "-infinity":
		*t = Timestamptz{Status: Present, InfinityModifier: -Infinity}
	default:
		var format string
		if sbuf[len(sbuf)-9] == '-' || sbuf[len(sbuf)-9] == '+' {
			format = pgTimestamptzSecondFormat
		} else if sbuf[len(sbuf)-6] == '-' || sbuf[len(sbuf)-6] == '+' {
			format = pgTimestamptzMinuteFormat
		} else {
			format = pgTimestamptzHourFormat
		}

		tim, err := time.Parse(format, sbuf)
		if err != nil {
			return err
		}

		*t = Timestamptz{Time: tim, Status: Present}
	}

	return nil
}

func (t *Timestamptz) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*t = Timestamptz{Status: Null}
		return nil
	}

	if size != 8 {
		return fmt.Errorf("invalid length for timestamptz: %v", size)
	}

	microsecSinceY2K, err := pgio.ReadInt64(r)
	if err != nil {
		return err
	}

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		*t = Timestamptz{Status: Present, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		*t = Timestamptz{Status: Present, InfinityModifier: -Infinity}
	default:
		microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
		tim := time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
		*t = Timestamptz{Time: tim, Status: Present}
	}

	return nil
}

func (t Timestamptz) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, t.Status); done {
		return err
	}

	var s string

	switch t.InfinityModifier {
	case None:
		s = t.Time.UTC().Format(pgTimestamptzSecondFormat)
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

func (t Timestamptz) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, t.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 8)
	if err != nil {
		return err
	}

	var microsecSinceY2K int64
	switch t.InfinityModifier {
	case None:
		microsecSinceUnixEpoch := t.Time.Unix()*1000000 + int64(t.Time.Nanosecond())/1000
		microsecSinceY2K = microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
	case Infinity:
		microsecSinceY2K = infinityMicrosecondOffset
	case NegativeInfinity:
		microsecSinceY2K = negativeInfinityMicrosecondOffset
	}

	_, err = pgio.WriteInt64(w, microsecSinceY2K)
	return err
}
