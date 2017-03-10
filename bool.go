package pgtype

import (
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Bool struct {
	Bool   bool
	Status Status
}

func (dst *Bool) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Bool:
		*dst = value
	case bool:
		*dst = Bool{Bool: value, Status: Present}
	case string:
		bb, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		*dst = Bool{Bool: bb, Status: Present}
	default:
		if originalSrc, ok := underlyingBoolType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Bool", value)
	}

	return nil
}

func (src *Bool) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *bool:
		if src.Status != Present {
			return fmt.Errorf("cannot assign %v to %T", src, dst)
		}
		*v = src.Bool
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
			case reflect.Bool:
				if src.Status != Present {
					return fmt.Errorf("cannot assign %v to %T", src, dst)
				}
				el.SetBool(src.Bool)
				return nil
			}
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *Bool) DecodeText(src []byte) error {
	if src == nil {
		*dst = Bool{Status: Null}
		return nil
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	*dst = Bool{Bool: src[0] == 't', Status: Present}
	return nil
}

func (dst *Bool) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Bool{Status: Null}
		return nil
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	*dst = Bool{Bool: src[0] == 1, Status: Present}
	return nil
}

func (src Bool) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 1)
	if err != nil {
		return nil
	}

	var buf []byte
	if src.Bool {
		buf = []byte{'t'}
	} else {
		buf = []byte{'f'}
	}

	_, err = w.Write(buf)
	return err
}

func (src Bool) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 1)
	if err != nil {
		return nil
	}

	var buf []byte
	if src.Bool {
		buf = []byte{1}
	} else {
		buf = []byte{0}
	}

	_, err = w.Write(buf)
	return err
}
