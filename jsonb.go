package pgtype

import (
	"fmt"
	"io"
)

type Jsonb Json

func (dst *Jsonb) ConvertFrom(src interface{}) error {
	return (*Json)(dst).ConvertFrom(src)
}

func (src *Jsonb) AssignTo(dst interface{}) error {
	return (*Json)(src).AssignTo(dst)
}

func (dst *Jsonb) DecodeText(src []byte) error {
	return (*Json)(dst).DecodeText(src)
}

func (dst *Jsonb) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Jsonb{Status: Null}
		return nil
	}

	if len(src) == 0 {
		return fmt.Errorf("jsonb too short")
	}

	if src[0] != 1 {
		return fmt.Errorf("unknown jsonb version number %d", src[0])
	}
	src = src[1:]

	buf := make([]byte, len(src))
	copy(buf, src)

	*dst = Jsonb{Bytes: buf, Status: Present}
	return nil

}

func (src Jsonb) EncodeText(w io.Writer) (bool, error) {
	return (Json)(src).EncodeText(w)
}

func (src Jsonb) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := w.Write([]byte{1})
	if err != nil {
		return false, err
	}

	_, err = w.Write(src.Bytes)
	return false, err
}
