package pgtype

import (
	"fmt"
	"io"
	"reflect"
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

func (dst *Text) DecodeText(src []byte) error {
	if src == nil {
		*dst = Text{Status: Null}
		return nil
	}

	*dst = Text{String: string(src), Status: Present}
	return nil
}

func (dst *Text) DecodeBinary(src []byte) error {
	return dst.DecodeText(src)
}

func (src Text) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, src.String)
	return false, err
}

func (src Text) EncodeBinary(w io.Writer) (bool, error) {
	return src.EncodeText(w)
}
