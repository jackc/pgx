package pgtype

import (
	"fmt"
	"io"
	"reflect"

	"github.com/jackc/pgx/pgio"
)

type Text struct {
	String string
	Status Status
}

func (dst *Text) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Text:
		*dst = value
	case string:
		*dst = Text{String: value, Status: Present}
	case *string:
		if value == nil {
			*dst = Text{Status: Null}
		} else {
			*dst = Text{String: *value, Status: Present}
		}
	default:
		if originalSrc, ok := underlyingStringType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Text", value)
	}

	return nil
}

func (src *Text) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *string:
		if src.Status != Present {
			return fmt.Errorf("cannot assign %v to %T", src, dst)
		}
		*v = src.String
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
			case reflect.String:
				if src.Status != Present {
					return fmt.Errorf("cannot assign %v to %T", src, dst)
				}
				el.SetString(src.String)
				return nil
			}
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *Text) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*dst = Text{Status: Null}
		return nil
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	*dst = Text{String: string(buf), Status: Present}
	return nil
}

func (dst *Text) DecodeBinary(r io.Reader) error {
	return dst.DecodeText(r)
}

func (src Text) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, int32(len(src.String)))
	if err != nil {
		return nil
	}

	_, err = io.WriteString(w, src.String)
	return err
}

func (src Text) EncodeBinary(w io.Writer) error {
	return src.EncodeText(w)
}
