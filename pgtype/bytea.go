package pgtype

import (
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
)

type Bytea struct {
	Bytes  []byte
	Status Status
}

func (dst *Bytea) Set(src interface{}) error {
	switch value := src.(type) {
	case Bytea:
		*dst = value
	case []byte:
		if value != nil {
			*dst = Bytea{Bytes: value, Status: Present}
		} else {
			*dst = Bytea{Status: Null}
		}
	default:
		if originalSrc, ok := underlyingBytesType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Bytea", value)
	}

	return nil
}

func (dst *Bytea) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Bytes
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Bytea) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *[]byte:
		if src.Status == Present {
			*v = src.Bytes
		} else {
			*v = nil
		}
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
			default:
				if originalDst, ok := underlyingPtrSliceType(dst); ok {
					return src.AssignTo(originalDst)
				}
			}
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

// DecodeText only supports the hex format. This has been the default since
// PostgreSQL 9.0.
func (dst *Bytea) DecodeText(src []byte) error {
	if src == nil {
		*dst = Bytea{Status: Null}
		return nil
	}

	if len(src) < 2 || src[0] != '\\' || src[1] != 'x' {
		return fmt.Errorf("invalid hex format")
	}

	buf := make([]byte, (len(src)-2)/2)
	_, err := hex.Decode(buf, src[2:])
	if err != nil {
		return err
	}

	*dst = Bytea{Bytes: buf, Status: Present}
	return nil
}

func (dst *Bytea) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Bytea{Status: Null}
		return nil
	}

	*dst = Bytea{Bytes: src, Status: Present}
	return nil
}

func (src Bytea) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, `\x`)
	if err != nil {
		return false, err
	}

	_, err = io.WriteString(w, hex.EncodeToString(src.Bytes))
	return false, err
}

func (src Bytea) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := w.Write(src.Bytes)
	return false, err
}
