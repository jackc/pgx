package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/jackc/pgx/pgio"
)

const pgTimestampFormat = "2006-01-02 15:04:05.999999999"

// Timestamp represents the PostgreSQL timestamp type. The PostgreSQL
// timestamp does not have a time zone. This presents a problem when
// translating to and from time.Time which requires a time zone. It is highly
// recommended to use timestamptz whenever possible. Timestamp methods either
// convert to UTC or return an error on non-UTC times.
type Timestamp struct {
	Time   time.Time // Time must always be in UTC.
	Status Status
	InfinityModifier
}

// ConvertFrom converts src into a Timestamp and stores in dst. If src is a
// time.Time in a non-UTC time zone, the time zone is discarded.
func (dst *Timestamp) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Timestamp:
		*dst = value
	case time.Time:
		*dst = Timestamp{Time: time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), time.UTC), Status: Present}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Timestamp", value)
	}

	return nil
}

func (src *Timestamp) AssignTo(dst interface{}) error {
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
		return fmt.Errorf("cannot assign %v into %T", src, dst)
	}

	return nil
}

// DecodeText decodes from src into dst. The decoded time is considered to
// be in UTC.
func (dst *Timestamp) DecodeText(src []byte) error {
	if src == nil {
		*dst = Timestamp{Status: Null}
		return nil
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		*dst = Timestamp{Status: Present, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Timestamp{Status: Present, InfinityModifier: -Infinity}
	default:
		tim, err := time.Parse(pgTimestampFormat, sbuf)
		if err != nil {
			return err
		}

		*dst = Timestamp{Time: tim, Status: Present}
	}

	return nil
}

// DecodeBinary decodes from src into dst. The decoded time is considered to
// be in UTC.
func (dst *Timestamp) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Timestamp{Status: Null}
		return nil
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for timestamp: %v", len(src))
	}

	microsecSinceY2K := int64(binary.BigEndian.Uint64(src))

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		*dst = Timestamp{Status: Present, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		*dst = Timestamp{Status: Present, InfinityModifier: -Infinity}
	default:
		microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
		tim := time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000).UTC()
		*dst = Timestamp{Time: tim, Status: Present}
	}

	return nil
}

// EncodeText writes the text encoding of src into w. If src.Time is not in
// the UTC time zone it returns an error.
func (src Timestamp) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}
	if src.Time.Location() != time.UTC {
		return fmt.Errorf("cannot encode non-UTC time into timestamp")
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.Format(pgTimestampFormat)
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

// EncodeBinary writes the binary encoding of src into w. If src.Time is not in
// the UTC time zone it returns an error.
func (src Timestamp) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}
	if src.Time.Location() != time.UTC {
		return fmt.Errorf("cannot encode non-UTC time into timestamp")
	}

	_, err := pgio.WriteInt32(w, 8)
	if err != nil {
		return err
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

	_, err = pgio.WriteInt64(w, microsecSinceY2K)
	return err
}
