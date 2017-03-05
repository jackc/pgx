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
type XID CID

func (dst *XID) ConvertFrom(src interface{}) error {
	return (*CID)(dst).ConvertFrom(src)
}

func (src *XID) AssignTo(dst interface{}) error {
	return (*CID)(src).AssignTo(dst)
}

func (dst *XID) DecodeText(r io.Reader) error {
	return (*CID)(dst).DecodeText(r)
}

func (dst *XID) DecodeBinary(r io.Reader) error {
	return (*CID)(dst).DecodeBinary(r)
}

func (src XID) EncodeText(w io.Writer) error {
	return (CID)(src).EncodeText(w)
}

func (src XID) EncodeBinary(w io.Writer) error {
	return (CID)(src).EncodeBinary(w)
}
