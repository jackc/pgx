package pgtype

import (
	"database/sql/driver"
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

func (dst *Cid) DecodeText(ci *ConnInfo, src []byte) error {
	return (*pguint32)(dst).DecodeText(ci, src)
}

func (dst *Cid) DecodeBinary(ci *ConnInfo, src []byte) error {
	return (*pguint32)(dst).DecodeBinary(ci, src)
}

func (src Cid) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeText(ci, w)
}

func (src Cid) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	return (pguint32)(src).EncodeBinary(ci, w)
}

// Scan implements the database/sql Scanner interface.
func (dst *Cid) Scan(src interface{}) error {
	return (*pguint32)(dst).Scan(src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Cid) Value() (driver.Value, error) {
	return (pguint32)(src).Value()
}
