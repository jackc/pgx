package pgtype

import (
	"reflect"

	"github.com/pkg/errors"
)

// PostgreSQL oids for common types
const (
	BoolOID             = 16
	ByteaOID            = 17
	CharOID             = 18
	NameOID             = 19
	Int8OID             = 20
	Int2OID             = 21
	Int4OID             = 23
	TextOID             = 25
	OIDOID              = 26
	TIDOID              = 27
	XIDOID              = 28
	CIDOID              = 29
	JSONOID             = 114
	Unknown115OID       = 115
	PointOID            = 600
	LineSegmentOID      = 601
	PathOID             = 602
	BoxOID              = 603
	PolygonOID          = 604
	LineOID             = 628
	CIDROID             = 650
	CIDRArrayOID        = 651
	Float4OID           = 700
	Float8OID           = 701
	UnknownOID          = 705
	CircleOID           = 718
	MacaddrOID          = 829
	InetOID             = 869
	BoolArrayOID        = 1000
	Int2ArrayOID        = 1005
	Int4ArrayOID        = 1007
	TextArrayOID        = 1009
	ByteaArrayOID       = 1001
	VarcharArrayOID     = 1015
	Int8ArrayOID        = 1016
	Float4ArrayOID      = 1021
	Float8ArrayOID      = 1022
	ACLItemOID          = 1033
	ACLItemArrayOID     = 1034
	InetArrayOID        = 1041
	VarcharOID          = 1043
	DateOID             = 1082
	TimestampOID        = 1114
	TimestampArrayOID   = 1115
	DateArrayOID        = 1182
	TimestamptzOID      = 1184
	TimestamptzArrayOID = 1185
	IntervalOID         = 1186
	BitOID              = 1560
	VarbitOID           = 1562
	NumericOID          = 1700
	NumericArrayOID     = 1231
	RecordOID           = 2249
	UUIDOID             = 2950
	UUIDArrayOID        = 2951
	JSONBOID            = 3802
	Int4RangeOID        = 3904
	NumrangeOID         = 3906
	TsrangeOID          = 3908
	TstzrangeOID        = 3910
	DateRangeOID        = 3912
	Int8RangeOID        = 3926
)

// list of well known postgresql oid
var postgresqlDefinedOIDs = []OID{
	ACLItemArrayOID,
	BoolArrayOID,
	ByteaArrayOID,
	CIDRArrayOID,
	DateArrayOID,
	Float4ArrayOID,
	Float8ArrayOID,
	InetArrayOID,
	Int2ArrayOID,
	Int4ArrayOID,
	Int8ArrayOID,
	NumericArrayOID,
	TextArrayOID,
	TimestampArrayOID,
	TimestamptzArrayOID,
	UUIDArrayOID,
	VarcharArrayOID,
	ACLItemOID,
	BitOID,
	BoolOID,
	BoxOID,
	ByteaOID,
	CharOID,
	CIDOID,
	CIDROID,
	CircleOID,
	DateOID,
	DateRangeOID,
	Float4OID,
	Float8OID,
	InetOID,
	Int2OID,
	Int4OID,
	Int4RangeOID,
	Int8OID,
	Int8RangeOID,
	IntervalOID,
	JSONOID,
	JSONBOID,
	LineOID,
	LineSegmentOID,
	MacaddrOID,
	NameOID,
	NumericOID,
	NumrangeOID,
	OIDOID,
	PathOID,
	PointOID,
	PolygonOID,
	RecordOID,
	TextOID,
	TIDOID,
	TimestampOID,
	TimestamptzOID,
	TsrangeOID,
	TstzrangeOID,
	UnknownOID,
	UUIDOID,
	VarbitOID,
	VarcharOID,
	XIDOID,
}

// PostgreSQL format codes
const (
	TextFormatCode   int16 = 0
	BinaryFormatCode       = 1
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

	// AssignTo converts and assigns the Value to dst. It MUST make a deep copy of
	// any reference types.
	AssignTo(dst interface{}) error
}

type BinaryDecoder interface {
	// DecodeBinary decodes src into BinaryDecoder. If src is nil then the
	// original SQL value is NULL. BinaryDecoder takes ownership of src. The
	// caller MUST not use it again.
	DecodeBinary(ci *ConnInfo, src []byte) error
}

type TextDecoder interface {
	// DecodeText decodes src into TextDecoder. If src is nil then the original
	// SQL value is NULL. TextDecoder takes ownership of src. The caller MUST not
	// use it again.
	DecodeText(ci *ConnInfo, src []byte) error
}

// BinaryEncoder is implemented by types that can encode themselves into the
// PostgreSQL binary wire format.
type BinaryEncoder interface {
	// EncodeBinary should append the binary format of self to buf. If self is the
	// SQL value NULL then append nothing and return (nil, nil). The caller of
	// EncodeBinary is responsible for writing the correct NULL value or the
	// length of the data written.
	EncodeBinary(ci *ConnInfo, buf []byte) (newBuf []byte, err error)
}

// TextEncoder is implemented by types that can encode themselves into the
// PostgreSQL text wire format.
type TextEncoder interface {
	// EncodeText should append the text format of self to buf. If self is the
	// SQL value NULL then append nothing and return (nil, nil). The caller of
	// EncodeText is responsible for writing the correct NULL value or the
	// length of the data written.
	EncodeText(ci *ConnInfo, buf []byte) (newBuf []byte, err error)
}

var errUndefined = errors.New("cannot encode status undefined")
var errBadStatus = errors.New("invalid status")

type DataType struct {
	Value      Value
	Name       string
	OID        OID
	FormatCode int16
}

type ConnInfo struct {
	oidToDataType         map[OID]*DataType
	nameToDataType        map[string]*DataType
	reflectTypeToDataType map[reflect.Type]*DataType
}

func NewConnInfo() *ConnInfo {
	return &ConnInfo{
		oidToDataType:         make(map[OID]*DataType, 256),
		nameToDataType:        make(map[string]*DataType, 256),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, 256),
	}
}

func (ci *ConnInfo) DataTypes() map[OID]DataType {
	out := make(map[OID]DataType, len(ci.oidToDataType))
	for _, dt := range ci.oidToDataType {
		tmp := *dt
		out[dt.OID] = tmp
	}

	return out
}

func (ci *ConnInfo) InitializeDataTypes(nameOIDs map[string]OID) {
	for name, oid := range nameOIDs {
		var (
			value Value
		)

		if t, ok := nameValues[name]; ok {
			value = reflect.New(reflect.ValueOf(t).Elem().Type()).Interface().(Value)
		} else {
			value = &GenericText{}
		}

		ci.RegisterDataType(DataType{Value: value, Name: name, OID: oid, FormatCode: DetermineFormatCode(value)})
	}
}

func (ci *ConnInfo) RegisterDataType(t DataType) {
	ci.oidToDataType[t.OID] = &t
	ci.nameToDataType[t.Name] = &t
	ci.reflectTypeToDataType[reflect.ValueOf(t.Value).Type()] = &t
}

func (ci *ConnInfo) DataTypeForOID(oid OID) (*DataType, bool) {
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
		oidToDataType:         make(map[OID]*DataType, len(ci.oidToDataType)),
		nameToDataType:        make(map[string]*DataType, len(ci.nameToDataType)),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, len(ci.reflectTypeToDataType)),
	}

	for _, dt := range ci.oidToDataType {
		ci2.RegisterDataType(DataType{
			Value:      reflect.New(reflect.ValueOf(dt.Value).Elem().Type()).Interface().(Value),
			Name:       dt.Name,
			OID:        dt.OID,
			FormatCode: dt.FormatCode,
		})
	}

	return ci2
}

var nameValues map[string]Value

func init() {
	nameValues = make(map[string]Value, len(postgresqlDefinedOIDs))
	for _, oid := range postgresqlDefinedOIDs {
		nameValues[oidName(oid)] = oidValue(oid)
	}

	// well known oid types by name.
	nameValues["decimal"] = &Decimal{}
	nameValues["hstore"] = &Hstore{}
}

func oidValue(oid OID) Value {
	switch oid {
	case ACLItemArrayOID:
		return &ACLItemArray{}
	case BoolArrayOID:
		return &BoolArray{}
	case ByteaArrayOID:
		return &ByteaArray{}
	case CIDRArrayOID:
		return &CIDRArray{}
	case DateArrayOID:
		return &DateArray{}
	case Float4ArrayOID:
		return &Float4Array{}
	case Float8ArrayOID:
		return &Float8Array{}
	case InetArrayOID:
		return &InetArray{}
	case Int2ArrayOID:
		return &Int2Array{}
	case Int4ArrayOID:
		return &Int4Array{}
	case Int8ArrayOID:
		return &Int8Array{}
	case NumericArrayOID:
		return &NumericArray{}
	case TextArrayOID:
		return &TextArray{}
	case TimestampArrayOID:
		return &TimestampArray{}
	case TimestamptzArrayOID:
		return &TimestamptzArray{}
	case UUIDArrayOID:
		return &UUIDArray{}
	case VarcharArrayOID:
		return &VarcharArray{}
	case ACLItemOID:
		return &ACLItem{}
	case BitOID:
		return &Bit{}
	case BoolOID:
		return &Bool{}
	case BoxOID:
		return &Box{}
	case ByteaOID:
		return &Bytea{}
	case CharOID:
		return &QChar{}
	case CIDOID:
		return &CID{}
	case CIDROID:
		return &CIDR{}
	case CircleOID:
		return &Circle{}
	case DateOID:
		return &Date{}
	case DateRangeOID:
		return &Daterange{}
	case Float4OID:
		return &Float4{}
	case Float8OID:
		return &Float8{}
	case InetOID:
		return &Inet{}
	case Int2OID:
		return &Int2{}
	case Int4OID:
		return &Int4{}
	case Int4RangeOID:
		return &Int4range{}
	case Int8OID:
		return &Int8{}
	case Int8RangeOID:
		return &Int8range{}
	case IntervalOID:
		return &Interval{}
	case JSONOID:
		return &JSON{}
	case JSONBOID:
		return &JSONB{}
	case LineOID:
		return &Line{}
	case LineSegmentOID:
		return &Lseg{}
	case MacaddrOID:
		return &Macaddr{}
	case NameOID:
		return &Name{}
	case NumericOID:
		return &Numeric{}
	case NumrangeOID:
		return &Numrange{}
	case OIDOID:
		return &OIDValue{}
	case PathOID:
		return &Path{}
	case PointOID:
		return &Point{}
	case PolygonOID:
		return &Polygon{}
	case RecordOID:
		return &Record{}
	case TextOID:
		return &Text{}
	case TIDOID:
		return &TID{}
	case TimestampOID:
		return &Timestamp{}
	case TimestamptzOID:
		return &Timestamptz{}
	case TsrangeOID:
		return &Tsrange{}
	case TstzrangeOID:
		return &Tstzrange{}
	case UnknownOID:
		return &Unknown{}
	case UUIDOID:
		return &UUID{}
	case VarbitOID:
		return &Varbit{}
	case VarcharOID:
		return &Varchar{}
	case XIDOID:
		return &XID{}
	default:
		return &GenericText{}
	}
}

// oidName returns well known names for the given oid if known.
func oidName(oid OID) string {
	switch oid {
	case ACLItemArrayOID:
		return "_aclitem"
	case BoolArrayOID:
		return "_bool"
	case ByteaArrayOID:
		return "_bytea"
	case CIDRArrayOID:
		return "_cidr"
	case DateArrayOID:
		return "_date"
	case Float4ArrayOID:
		return "_float4"
	case Float8ArrayOID:
		return "_float8"
	case InetArrayOID:
		return "_inet"
	case Int2ArrayOID:
		return "_int2"
	case Int4ArrayOID:
		return "_int4"
	case Int8ArrayOID:
		return "_int8"
	case NumericArrayOID:
		return "_numeric"
	case TextArrayOID:
		return "_text"
	case TimestampArrayOID:
		return "_timestamp"
	case TimestamptzArrayOID:
		return "_timestamptz"
	case UUIDArrayOID:
		return "_uuid"
	case VarcharArrayOID:
		return "_varchar"
	case ACLItemOID:
		return "aclitem"
	case BitOID:
		return "bit"
	case BoolOID:
		return "bool"
	case BoxOID:
		return "box"
	case ByteaOID:
		return "bytea"
	case CharOID:
		return "char"
	case CIDOID:
		return "cid"
	case CIDROID:
		return "cidr"
	case CircleOID:
		return "circle"
	case DateOID:
		return "date"
	case DateRangeOID:
		return "daterange"
	case Float4OID:
		return "float4"
	case Float8OID:
		return "float8"
	case InetOID:
		return "inet"
	case Int2OID:
		return "int2"
	case Int4OID:
		return "int4"
	case Int4RangeOID:
		return "int4range"
	case Int8OID:
		return "int8"
	case Int8RangeOID:
		return "int8range"
	case IntervalOID:
		return "interval"
	case JSONOID:
		return "json"
	case JSONBOID:
		return "jsonb"
	case LineOID:
		return "line"
	case LineSegmentOID:
		return "lseg"
	case MacaddrOID:
		return "macaddr"
	case NameOID:
		return "name"
	case NumericOID:
		return "numeric"
	case NumrangeOID:
		return "numrange"
	case OIDOID:
		return "oid"
	case PathOID:
		return "path"
	case PointOID:
		return "point"
	case PolygonOID:
		return "polygon"
	case RecordOID:
		return "record"
	case TextOID:
		return "text"
	case TIDOID:
		return "tid"
	case TimestampOID:
		return "timestamp"
	case TimestamptzOID:
		return "timestamptz"
	case TsrangeOID:
		return "tsrange"
	case TstzrangeOID:
		return "tstzrange"
	case UnknownOID:
		return "unknown"
	case UUIDOID:
		return "uuid"
	case VarbitOID:
		return "varbit"
	case VarcharOID:
		return "varchar"
	case XIDOID:
		return "xid"
	default:
		return ""
	}
}

// DetermineFormatCode determines the default format code to use
// for the given value.
func DetermineFormatCode(v Value) int16 {
	if _, ok := v.(BinaryDecoder); ok {
		return BinaryFormatCode
	}

	return TextFormatCode
}

func NewPGBouncerConnInfo() *ConnInfo {
	info := NewConnInfo()
	nameOIDs := make(map[string]OID, len(postgresqlDefinedOIDs))
	for _, oid := range postgresqlDefinedOIDs {
		nameOIDs[oidName(oid)] = oid
	}
	info.InitializeDataTypes(nameOIDs)
	return info
}
