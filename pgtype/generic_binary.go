package pgtype

import (
	"io"
)

// GenericBinary is a placeholder for binary format values that no other type exists
// to handle.
type GenericBinary Bytea

func (dst *GenericBinary) Set(src interface{}) error {
	return (*Bytea)(dst).Set(src)
}

func (dst *GenericBinary) Get() interface{} {
	return (*Bytea)(dst).Get()
}

func (src *GenericBinary) AssignTo(dst interface{}) error {
	return (*Bytea)(src).AssignTo(dst)
}

func (dst *GenericBinary) DecodeBinary(ci *ConnInfo, src []byte) error {
	return (*Bytea)(dst).DecodeBinary(ci, src)
}

func (src GenericBinary) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	return (Bytea)(src).EncodeBinary(ci, w)
}
