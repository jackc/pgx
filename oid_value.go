package pgtype

import (
	"io"
)

// OidValue (Object Identifier Type) is, according to
// https://www.postgresql.org/docs/current/static/datatype-OidValue.html, used
// internally by PostgreSQL as a primary key for various system tables. It is
// currently implemented as an unsigned four-byte integer. Its definition can be
// found in src/include/postgres_ext.h in the PostgreSQL sources.
type OidValue pguint32

// Set converts from src to dst. Note that as OidValue is not a general
// number type Set does not do automatic type conversion as other number
// types do.
func (dst *OidValue) Set(src interface{}) error {
	return (*pguint32)(dst).Set(src)
}

func (dst *OidValue) Get() interface{} {
	return (*pguint32)(dst).Get()
}

// AssignTo assigns from src to dst. Note that as OidValue is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *OidValue) AssignTo(dst interface{}) error {
	return (*pguint32)(src).AssignTo(dst)
}

func (dst *OidValue) DecodeText(src []byte) error {
	return (*pguint32)(dst).DecodeText(src)
}

func (dst *OidValue) DecodeBinary(src []byte) error {
	return (*pguint32)(dst).DecodeBinary(src)
}

func (src OidValue) EncodeText(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeText(w)
}

func (src OidValue) EncodeBinary(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeBinary(w)
}
