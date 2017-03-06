package pgtype

import (
	"io"
)

// Name is a type used for PostgreSQL's special 63-byte
// name data type, used for identifiers like table names.
// The pg_class.relname column is a good example of where the
// name data type is used.
//
// Note that the underlying Go data type of pgx.Name is string,
// so there is no way to enforce the 63-byte length. Inputting
// a longer name into PostgreSQL will result in silent truncation
// to 63 bytes.
//
// Also, if you have custom-compiled PostgreSQL and set
// NAMEDATALEN to a different value, obviously that number of
// bytes applies, rather than the default 63.
type Name Text

func (dst *Name) ConvertFrom(src interface{}) error {
	return (*Text)(dst).ConvertFrom(src)
}

func (src *Name) AssignTo(dst interface{}) error {
	return (*Text)(src).AssignTo(dst)
}

func (dst *Name) DecodeText(r io.Reader) error {
	return (*Text)(dst).DecodeText(r)
}

func (dst *Name) DecodeBinary(r io.Reader) error {
	return (*Text)(dst).DecodeBinary(r)
}

func (src Name) EncodeText(w io.Writer) error {
	return (Text)(src).EncodeText(w)
}

func (src Name) EncodeBinary(w io.Writer) error {
	return (Text)(src).EncodeBinary(w)
}
