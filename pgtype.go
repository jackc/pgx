package pgtype

import (
	"errors"
	"io"
	"reflect"
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

func (im InfinityModifier) String() string {
	switch im {
	case None:
		return "none"
	case Infinity:
		return "infinity"
	case NegativeInfinity:
		return "-infinity"
	default:
		return "invalid"
	}
}

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
	// DecodeBinary decodes src into BinaryDecoder. If src is nil then the
	// original SQL value is NULL. BinaryDecoder MUST not retain a reference to
	// src. It MUST make a copy if it needs to retain the raw bytes.
	DecodeBinary(ci *ConnInfo, src []byte) error
}

type TextDecoder interface {
	// DecodeText decodes src into TextDecoder. If src is nil then the original
	// SQL value is NULL. TextDecoder MUST not retain a reference to src. It MUST
	// make a copy if it needs to retain the raw bytes.
	DecodeText(ci *ConnInfo, src []byte) error
}

// BinaryEncoder is implemented by types that can encode themselves into the
// PostgreSQL binary wire format.
type BinaryEncoder interface {
	// EncodeBinary should encode the binary format of self to w. If self is the
	// SQL value NULL then write nothing and return (true, nil). The caller of
	// EncodeBinary is responsible for writing the correct NULL value or the
	// length of the data written.
	EncodeBinary(ci *ConnInfo, w io.Writer) (null bool, err error)
}

// TextEncoder is implemented by types that can encode themselves into the
// PostgreSQL text wire format.
type TextEncoder interface {
	// EncodeText should encode the text format of self to w. If self is the SQL
	// value NULL then write nothing and return (true, nil). The caller of
	// EncodeText is responsible for writing the correct NULL value or the length
	// of the data written.
	EncodeText(ci *ConnInfo, w io.Writer) (null bool, err error)
}

var errUndefined = errors.New("cannot encode status undefined")

type DataType struct {
	Value Value
	Name  string
	Oid   Oid
}

type ConnInfo struct {
	oidToDataType         map[Oid]*DataType
	nameToDataType        map[string]*DataType
	reflectTypeToDataType map[reflect.Type]*DataType
}

func NewConnInfo() *ConnInfo {
	return &ConnInfo{
		oidToDataType:         make(map[Oid]*DataType, 256),
		nameToDataType:        make(map[string]*DataType, 256),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, 256),
	}
}

func (ci *ConnInfo) InitializeDataTypes(nameOids map[string]Oid) {
	for name, oid := range nameOids {
		var value Value
		if t, ok := nameValues[name]; ok {
			value = reflect.New(reflect.ValueOf(t).Elem().Type()).Interface().(Value)
		} else {
			value = &GenericText{}
		}
		ci.RegisterDataType(DataType{Value: value, Name: name, Oid: oid})
	}
}

func (ci *ConnInfo) RegisterDataType(t DataType) {
	ci.oidToDataType[t.Oid] = &t
	ci.nameToDataType[t.Name] = &t
	ci.reflectTypeToDataType[reflect.ValueOf(t.Value).Type()] = &t
}

func (ci *ConnInfo) DataTypeForOid(oid Oid) (*DataType, bool) {
	dt, ok := ci.oidToDataType[oid]
	return dt, ok
}

func (ci *ConnInfo) DataTypeForName(name string) (*DataType, bool) {
	dt, ok := ci.nameToDataType[name]
	return dt, ok
}

func (ci *ConnInfo) DataTypeForValue(v Value) (*DataType, bool) {
	dt, ok := ci.reflectTypeToDataType[reflect.ValueOf(v).Type()]
	return dt, ok
}

// DeepCopy makes a deep copy of the ConnInfo.
func (ci *ConnInfo) DeepCopy() *ConnInfo {
	ci2 := &ConnInfo{
		oidToDataType:         make(map[Oid]*DataType, len(ci.oidToDataType)),
		nameToDataType:        make(map[string]*DataType, len(ci.nameToDataType)),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, len(ci.reflectTypeToDataType)),
	}

	for _, dt := range ci.oidToDataType {
		ci2.RegisterDataType(DataType{
			Value: reflect.New(reflect.ValueOf(dt.Value).Elem().Type()).Interface().(Value),
			Name:  dt.Name,
			Oid:   dt.Oid,
		})
	}

	return ci2
}

var nameValues map[string]Value

func init() {
	nameValues = map[string]Value{
		"_aclitem":     &AclitemArray{},
		"_bool":        &BoolArray{},
		"_bytea":       &ByteaArray{},
		"_cidr":        &CidrArray{},
		"_date":        &DateArray{},
		"_float4":      &Float4Array{},
		"_float8":      &Float8Array{},
		"_inet":        &InetArray{},
		"_int2":        &Int2Array{},
		"_int4":        &Int4Array{},
		"_int8":        &Int8Array{},
		"_numeric":     &NumericArray{},
		"_text":        &TextArray{},
		"_timestamp":   &TimestampArray{},
		"_timestamptz": &TimestamptzArray{},
		"_varchar":     &VarcharArray{},
		"aclitem":      &Aclitem{},
		"bool":         &Bool{},
		"bytea":        &Bytea{},
		"char":         &QChar{},
		"cid":          &Cid{},
		"cidr":         &Cidr{},
		"date":         &Date{},
		"daterange":    &Daterange{},
		"decimal":      &Decimal{},
		"float4":       &Float4{},
		"float8":       &Float8{},
		"hstore":       &Hstore{},
		"inet":         &Inet{},
		"int2":         &Int2{},
		"int4":         &Int4{},
		"int4range":    &Int4range{},
		"int8":         &Int8{},
		"int8range":    &Int8range{},
		"json":         &Json{},
		"jsonb":        &Jsonb{},
		"name":         &Name{},
		"numeric":      &Numeric{},
		"numrange":     &Numrange{},
		"oid":          &OidValue{},
		"point":        &Point{},
		"record":       &Record{},
		"text":         &Text{},
		"tid":          &Tid{},
		"timestamp":    &Timestamp{},
		"timestamptz":  &Timestamptz{},
		"tsrange":      &Tsrange{},
		"tstzrange":    &Tstzrange{},
		"unknown":      &Unknown{},
		"varchar":      &Varchar{},
		"xid":          &Xid{},
	}
}
