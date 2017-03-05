package pgtype

import (
	"io"
)

// CID is PostgreSQL's Command Identifier type.
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
type CID pguint32

// ConvertFrom converts from src to dst. Note that as CID is not a general
// number type ConvertFrom does not do automatic type conversion as other number
// types do.
func (dst *CID) ConvertFrom(src interface{}) error {
	return (*pguint32)(dst).ConvertFrom(src)
}

// AssignTo assigns from src to dst. Note that as CID is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *CID) AssignTo(dst interface{}) error {
	return (*pguint32)(src).AssignTo(dst)
}

func (dst *CID) DecodeText(r io.Reader) error {
	return (*pguint32)(dst).DecodeText(r)
}

func (dst *CID) DecodeBinary(r io.Reader) error {
	return (*pguint32)(dst).DecodeBinary(r)
}

func (src CID) EncodeText(w io.Writer) error {
	return (pguint32)(src).EncodeText(w)
}

func (src CID) EncodeBinary(w io.Writer) error {
	return (pguint32)(src).EncodeBinary(w)
}
