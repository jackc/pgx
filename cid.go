package pgtype

import (
	"io"
)

// Cid is PostgreSQL's Command Identifier type.
//
// When one does
//
// 	select cmin, cmax, * from some_table;
//
// it is the data type of the cmin and cmax hidden system columns.
//
// It is currently implemented as an unsigned four byte integer.
// Its definition can be found in src/include/c.h as CommandId
// in the PostgreSQL sources.
type Cid pguint32

// Set converts from src to dst. Note that as Cid is not a general
// number type Set does not do automatic type conversion as other number
// types do.
func (dst *Cid) Set(src interface{}) error {
	return (*pguint32)(dst).Set(src)
}

func (dst *Cid) Get() interface{} {
	return (*pguint32)(dst).Get()
}

// AssignTo assigns from src to dst. Note that as Cid is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *Cid) AssignTo(dst interface{}) error {
	return (*pguint32)(src).AssignTo(dst)
}

func (dst *Cid) DecodeText(src []byte) error {
	return (*pguint32)(dst).DecodeText(src)
}

func (dst *Cid) DecodeBinary(src []byte) error {
	return (*pguint32)(dst).DecodeBinary(src)
}

func (src Cid) EncodeText(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeText(w)
}

func (src Cid) EncodeBinary(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeBinary(w)
}
