package pgtype

import (
	"io"
)

// Xid is PostgreSQL's Transaction ID type.
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
type Xid pguint32

// Set converts from src to dst. Note that as Xid is not a general
// number type Set does not do automatic type conversion as other number
// types do.
func (dst *Xid) Set(src interface{}) error {
	return (*pguint32)(dst).Set(src)
}

func (dst *Xid) Get() interface{} {
	return (*pguint32)(dst).Get()
}

// AssignTo assigns from src to dst. Note that as Xid is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *Xid) AssignTo(dst interface{}) error {
	return (*pguint32)(src).AssignTo(dst)
}

func (dst *Xid) DecodeText(src []byte) error {
	return (*pguint32)(dst).DecodeText(src)
}

func (dst *Xid) DecodeBinary(src []byte) error {
	return (*pguint32)(dst).DecodeBinary(src)
}

func (src Xid) EncodeText(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeText(w)
}

func (src Xid) EncodeBinary(w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeBinary(w)
}
