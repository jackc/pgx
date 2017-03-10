package pgtype

import (
	"io"
)

// OID (Object Identifier Type) is, according to
// https://www.postgresql.org/docs/current/static/datatype-oid.html, used
// internally by PostgreSQL as a primary key for various system tables. It is
// currently implemented as an unsigned four-byte integer. Its definition can be
// found in src/include/postgres_ext.h in the PostgreSQL sources.
type OID pguint32

// ConvertFrom converts from src to dst. Note that as OID is not a general
// number type ConvertFrom does not do automatic type conversion as other number
// types do.
func (dst *OID) ConvertFrom(src interface{}) error {
	return (*pguint32)(dst).ConvertFrom(src)
}

// AssignTo assigns from src to dst. Note that as OID is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *OID) AssignTo(dst interface{}) error {
	return (*pguint32)(src).AssignTo(dst)
}

func (dst *OID) DecodeText(src []byte) error {
	return (*pguint32)(dst).DecodeText(src)
}

func (dst *OID) DecodeBinary(src []byte) error {
	return (*pguint32)(dst).DecodeBinary(src)
}

func (src OID) EncodeText(w io.Writer) error {
	return (pguint32)(src).EncodeText(w)
}

func (src OID) EncodeBinary(w io.Writer) error {
	return (pguint32)(src).EncodeBinary(w)
}
