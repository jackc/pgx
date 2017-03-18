package pgtype

import (
	"fmt"
	"io"
)

type Text struct {
	String string
	Status Status
}

func (dst *Text) Set(src interface{}) error {
	switch value := src.(type) {
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
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Text", value)
	}

	return nil
}

func (dst *Text) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.String
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Text) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *string:
			*v = src.String
			return nil
		case *[]byte:
			*v = make([]byte, len(src.String))
			copy(*v, src.String)
			return nil
		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
		}
	case Null:
		return nullAssignTo(dst)
	}

	return fmt.Errorf("cannot decode %v into %T", src, dst)
}

func (dst *Text) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Text{Status: Null}
		return nil
	}

	*dst = Text{String: string(src), Status: Present}
	return nil
}

func (dst *Text) DecodeBinary(ci *ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (src Text) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, src.String)
	return false, err
}

func (src Text) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	return src.EncodeText(ci, w)
}
