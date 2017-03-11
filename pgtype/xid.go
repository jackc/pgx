package pgtype

import (
	"io"
)

// XID is PostgreSQL's Transaction ID type.
//
// In later versions of PostgreSQL, it is the type used for the backend_xid
// and backend_xmin columns of the pg_stat_activity system view.
//
// Also, when one does
//
//  select xmin, xmax, * from some_table;
//
// it is the data type of the xmin and xmax hidden system columns.
//
// It is currently implemented as an unsigned four byte integer.
// Its definition can be found in src/include/postgres_ext.h as TransactionId
// in the PostgreSQL sources.
type XID pguint32

// ConvertFrom converts from src to dst. Note that as XID is not a general
// number type ConvertFrom does not do automatic type conversion as other number
// types do.
func (dst *XID) ConvertFrom(src interface{}) error {
	return (*pguint32)(dst).ConvertFrom(src)
}

// AssignTo assigns from src to dst. Note that as XID is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *XID) AssignTo(dst interface{}) error {
	return (*pguint32)(src).AssignTo(dst)
}

func (dst *XID) DecodeText(src []byte) error {
	return (*pguint32)(dst).DecodeText(src)
}

func (dst *XID) DecodeBinary(src []byte) error {
	return (*pguint32)(dst).DecodeBinary(src)
}

func (src XID) EncodeText(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeText(w)
}

func (src XID) EncodeBinary(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeBinary(w)
}
