package pgtype

import (
	"errors"
	"io"
)

// PostgreSQL oids for common types
const (
	BoolOid             = 16
	ByteaOid            = 17
	CharOid             = 18
	NameOid             = 19
	Int8Oid             = 20
	Int2Oid             = 21
	Int4Oid             = 23
	TextOid             = 25
	OidOid              = 26
	TidOid              = 27
	XidOid              = 28
	CidOid              = 29
	JsonOid             = 114
	CidrOid             = 650
	CidrArrayOid        = 651
	Float4Oid           = 700
	Float8Oid           = 701
	UnknownOid          = 705
	InetOid             = 869
	BoolArrayOid        = 1000
	Int2ArrayOid        = 1005
	Int4ArrayOid        = 1007
	TextArrayOid        = 1009
	ByteaArrayOid       = 1001
	VarcharArrayOid     = 1015
	Int8ArrayOid        = 1016
	Float4ArrayOid      = 1021
	Float8ArrayOid      = 1022
	AclitemOid          = 1033
	AclitemArrayOid     = 1034
	InetArrayOid        = 1041
	VarcharOid          = 1043
	DateOid             = 1082
	TimestampOid        = 1114
	TimestampArrayOid   = 1115
	DateArrayOid        = 1182
	TimestamptzOid      = 1184
	TimestamptzArrayOid = 1185
	RecordOid           = 2249
	UuidOid             = 2950
	JsonbOid            = 3802
)

type Status byte

const (
	Undefined Status = iota
	Null
	Present
)

type InfinityModifier int8

const (
	Infinity         InfinityModifier = 1
	None             InfinityModifier = 0
	NegativeInfinity InfinityModifier = -Infinity
)

type Value interface {
	// Set converts and assigns src to itself.
	Set(src interface{}) error

	// Get returns the simplest representation of Value. If the Value is Null or
	// Undefined that is the return value. If no simpler representation is
	// possible, then Get() returns Value.
	Get() interface{}

	// AssignTo converts and assigns the Value to dst.
	AssignTo(dst interface{}) error
}

type BinaryDecoder interface {
	DecodeBinary(src []byte) error
}

type TextDecoder interface {
	DecodeText(src []byte) error
}

// BinaryEncoder is implemented by types that can encode themselves into the
// PostgreSQL binary wire format.
type BinaryEncoder interface {
	// EncodeBinary should encode the binary format of self to w. If self is the
	// SQL value NULL then write nothing and return (true, nil). The caller of
	// EncodeBinary is responsible for writing the correct NULL value or the
	// length of the data written.
	EncodeBinary(w io.Writer) (null bool, err error)
}

// TextEncoder is implemented by types that can encode themselves into the
// PostgreSQL text wire format.
type TextEncoder interface {
	// EncodeText should encode the text format of self to w. If self is the SQL
	// value NULL then write nothing and return (true, nil). The caller of
	// EncodeText is responsible for writing the correct NULL value or the length
	// of the data written.
	EncodeText(w io.Writer) (null bool, err error)
}

var errUndefined = errors.New("cannot encode status undefined")
