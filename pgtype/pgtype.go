package pgtype

import (
	"io"
)

type Value interface {
	ConvertFrom(src interface{}) error
	AssignTo(dst interface{}) error
}

type BinaryDecoder interface {
	DecodeBinary(r io.Reader) error
}

type TextDecoder interface {
	DecodeText(r io.Reader) error
}

type BinaryEncoder interface {
	EncodeBinary(w io.Writer) error
}

type TextEncoder interface {
	EncodeText(w io.Writer) error
}
