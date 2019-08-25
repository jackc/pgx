package pgtype

import (
	"database/sql"
	"reflect"

	errors "golang.org/x/xerrors"
)

// PostgreSQL oids for common types
const (
	BoolOID             = 16
	ByteaOID            = 17
	QCharOID            = 18
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
	PointOID            = 600
	LsegOID             = 601
	PathOID             = 602
	BoxOID              = 603
	PolygonOID          = 604
	LineOID             = 628
	CIDROID             = 650
	CIDRArrayOID        = 651
	Float4OID           = 700
	Float8OID           = 701
	CircleOID           = 718
	UnknownOID          = 705
	MacaddrOID          = 829
	InetOID             = 869
	BoolArrayOID        = 1000
	Int2ArrayOID        = 1005
	Int4ArrayOID        = 1007
	TextArrayOID        = 1009
	ByteaArrayOID       = 1001
	BPCharArrayOID      = 1014
	VarcharArrayOID     = 1015
	Int8ArrayOID        = 1016
	Float4ArrayOID      = 1021
	Float8ArrayOID      = 1022
	ACLItemOID          = 1033
	ACLItemArrayOID     = 1034
	InetArrayOID        = 1041
	BPCharOID           = 1042
	VarcharOID          = 1043
	DateOID             = 1082
	TimestampOID        = 1114
	TimestampArrayOID   = 1115
	DateArrayOID        = 1182
	TimestamptzOID      = 1184
	TimestamptzArrayOID = 1185
	IntervalOID         = 1186
	NumericArrayOID     = 1231
	BitOID              = 1560
	VarbitOID           = 1562
	NumericOID          = 1700
	RecordOID           = 2249
	UUIDOID             = 2950
	UUIDArrayOID        = 2951
	JSONBOID            = 3802
	DaterangeOID        = 3812
	Int4rangeOID        = 3904
	NumrangeOID         = 3906
	TsrangeOID          = 3908
	TstzrangeOID        = 3910
	Int8rangeOID        = 3926
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

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

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
	Value Value
	Name  string
	OID   uint32
}

type ConnInfo struct {
	oidToDataType         map[uint32]*DataType
	nameToDataType        map[string]*DataType
	reflectTypeToDataType map[reflect.Type]*DataType
	oidToParamFormatCode  map[uint32]int16
	oidToResultFormatCode map[uint32]int16
}

func NewConnInfo() *ConnInfo {
	ci := &ConnInfo{
		oidToDataType:         make(map[uint32]*DataType, 128),
		nameToDataType:        make(map[string]*DataType, 128),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, 128),
		oidToParamFormatCode:  make(map[uint32]int16, 128),
		oidToResultFormatCode: make(map[uint32]int16, 128),
	}

	ci.RegisterDataType(DataType{Value: &ACLItemArray{}, Name: "_aclitem", OID: ACLItemArrayOID})
	ci.RegisterDataType(DataType{Value: &BoolArray{}, Name: "_bool", OID: BoolArrayOID})
	ci.RegisterDataType(DataType{Value: &BPCharArray{}, Name: "_bpchar", OID: BPCharArrayOID})
	ci.RegisterDataType(DataType{Value: &ByteaArray{}, Name: "_bytea", OID: ByteaArrayOID})
	ci.RegisterDataType(DataType{Value: &CIDRArray{}, Name: "_cidr", OID: CIDRArrayOID})
	ci.RegisterDataType(DataType{Value: &DateArray{}, Name: "_date", OID: DateArrayOID})
	ci.RegisterDataType(DataType{Value: &Float4Array{}, Name: "_float4", OID: Float4ArrayOID})
	ci.RegisterDataType(DataType{Value: &Float8Array{}, Name: "_float8", OID: Float8ArrayOID})
	ci.RegisterDataType(DataType{Value: &InetArray{}, Name: "_inet", OID: InetArrayOID})
	ci.RegisterDataType(DataType{Value: &Int2Array{}, Name: "_int2", OID: Int2ArrayOID})
	ci.RegisterDataType(DataType{Value: &Int4Array{}, Name: "_int4", OID: Int4ArrayOID})
	ci.RegisterDataType(DataType{Value: &Int8Array{}, Name: "_int8", OID: Int8ArrayOID})
	ci.RegisterDataType(DataType{Value: &NumericArray{}, Name: "_numeric", OID: NumericArrayOID})
	ci.RegisterDataType(DataType{Value: &TextArray{}, Name: "_text", OID: TextArrayOID})
	ci.RegisterDataType(DataType{Value: &TimestampArray{}, Name: "_timestamp", OID: TimestampArrayOID})
	ci.RegisterDataType(DataType{Value: &TimestamptzArray{}, Name: "_timestamptz", OID: TimestamptzArrayOID})
	ci.RegisterDataType(DataType{Value: &UUIDArray{}, Name: "_uuid", OID: UUIDArrayOID})
	ci.RegisterDataType(DataType{Value: &VarcharArray{}, Name: "_varchar", OID: VarcharArrayOID})
	ci.RegisterDataType(DataType{Value: &ACLItem{}, Name: "aclitem", OID: ACLItemOID})
	ci.RegisterDataType(DataType{Value: &Bit{}, Name: "bit", OID: BitOID})
	ci.RegisterDataType(DataType{Value: &Bool{}, Name: "bool", OID: BoolOID})
	ci.RegisterDataType(DataType{Value: &Box{}, Name: "box", OID: BoxOID})
	ci.RegisterDataType(DataType{Value: &BPChar{}, Name: "bpchar", OID: BPCharOID})
	ci.RegisterDataType(DataType{Value: &Bytea{}, Name: "bytea", OID: ByteaOID})
	ci.RegisterDataType(DataType{Value: &QChar{}, Name: "char", OID: QCharOID})
	ci.RegisterDataType(DataType{Value: &CID{}, Name: "cid", OID: CIDOID})
	ci.RegisterDataType(DataType{Value: &CIDR{}, Name: "cidr", OID: CIDROID})
	ci.RegisterDataType(DataType{Value: &Circle{}, Name: "circle", OID: CircleOID})
	ci.RegisterDataType(DataType{Value: &Date{}, Name: "date", OID: DateOID})
	ci.RegisterDataType(DataType{Value: &Daterange{}, Name: "daterange", OID: DaterangeOID})
	ci.RegisterDataType(DataType{Value: &Float4{}, Name: "float4", OID: Float4OID})
	ci.RegisterDataType(DataType{Value: &Float8{}, Name: "float8", OID: Float8OID})
	ci.RegisterDataType(DataType{Value: &Inet{}, Name: "inet", OID: InetOID})
	ci.RegisterDataType(DataType{Value: &Int2{}, Name: "int2", OID: Int2OID})
	ci.RegisterDataType(DataType{Value: &Int4{}, Name: "int4", OID: Int4OID})
	ci.RegisterDataType(DataType{Value: &Int4range{}, Name: "int4range", OID: Int4rangeOID})
	ci.RegisterDataType(DataType{Value: &Int8{}, Name: "int8", OID: Int8OID})
	ci.RegisterDataType(DataType{Value: &Int8range{}, Name: "int8range", OID: Int8rangeOID})
	ci.RegisterDataType(DataType{Value: &Interval{}, Name: "interval", OID: IntervalOID})
	ci.RegisterDataType(DataType{Value: &JSON{}, Name: "json", OID: JSONOID})
	ci.RegisterDataType(DataType{Value: &JSONB{}, Name: "jsonb", OID: JSONBOID})
	ci.RegisterDataType(DataType{Value: &Line{}, Name: "line", OID: LineOID})
	ci.RegisterDataType(DataType{Value: &Lseg{}, Name: "lseg", OID: LsegOID})
	ci.RegisterDataType(DataType{Value: &Macaddr{}, Name: "macaddr", OID: MacaddrOID})
	ci.RegisterDataType(DataType{Value: &Name{}, Name: "name", OID: NameOID})
	ci.RegisterDataType(DataType{Value: &Numeric{}, Name: "numeric", OID: NumericOID})
	ci.RegisterDataType(DataType{Value: &Numrange{}, Name: "numrange", OID: NumrangeOID})
	ci.RegisterDataType(DataType{Value: &OIDValue{}, Name: "oid", OID: OIDOID})
	ci.RegisterDataType(DataType{Value: &Path{}, Name: "path", OID: PathOID})
	ci.RegisterDataType(DataType{Value: &Point{}, Name: "point", OID: PointOID})
	ci.RegisterDataType(DataType{Value: &Polygon{}, Name: "polygon", OID: PolygonOID})
	ci.RegisterDataType(DataType{Value: &Record{}, Name: "record", OID: RecordOID})
	ci.RegisterDataType(DataType{Value: &Text{}, Name: "text", OID: TextOID})
	ci.RegisterDataType(DataType{Value: &TID{}, Name: "tid", OID: TIDOID})
	ci.RegisterDataType(DataType{Value: &Timestamp{}, Name: "timestamp", OID: TimestampOID})
	ci.RegisterDataType(DataType{Value: &Timestamptz{}, Name: "timestamptz", OID: TimestamptzOID})
	ci.RegisterDataType(DataType{Value: &Tsrange{}, Name: "tsrange", OID: TsrangeOID})
	ci.RegisterDataType(DataType{Value: &Tstzrange{}, Name: "tstzrange", OID: TstzrangeOID})
	ci.RegisterDataType(DataType{Value: &Unknown{}, Name: "unknown", OID: UnknownOID})
	ci.RegisterDataType(DataType{Value: &UUID{}, Name: "uuid", OID: UUIDOID})
	ci.RegisterDataType(DataType{Value: &Varbit{}, Name: "varbit", OID: VarbitOID})
	ci.RegisterDataType(DataType{Value: &Varchar{}, Name: "varchar", OID: VarcharOID})
	ci.RegisterDataType(DataType{Value: &XID{}, Name: "xid", OID: XIDOID})

	return ci
}

func (ci *ConnInfo) InitializeDataTypes(nameOIDs map[string]uint32) {
	for name, oid := range nameOIDs {
		var value Value
		if t, ok := nameValues[name]; ok {
			value = reflect.New(reflect.ValueOf(t).Elem().Type()).Interface().(Value)
		} else {
			value = &GenericText{}
		}
		ci.RegisterDataType(DataType{Value: value, Name: name, OID: oid})
	}
}

func (ci *ConnInfo) RegisterDataType(t DataType) {
	ci.oidToDataType[t.OID] = &t
	ci.nameToDataType[t.Name] = &t
	ci.reflectTypeToDataType[reflect.ValueOf(t.Value).Type()] = &t

	{
		var formatCode int16
		if _, ok := t.Value.(BinaryEncoder); ok {
			formatCode = BinaryFormatCode
		}
		ci.oidToParamFormatCode[t.OID] = formatCode
	}

	{
		var formatCode int16
		if _, ok := t.Value.(BinaryDecoder); ok {
			formatCode = BinaryFormatCode
		}
		ci.oidToResultFormatCode[t.OID] = formatCode
	}
}

func (ci *ConnInfo) DataTypeForOID(oid uint32) (*DataType, bool) {
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

func (ci *ConnInfo) ParamFormatCodeForOID(oid uint32) int16 {
	fc, ok := ci.oidToParamFormatCode[oid]
	if ok {
		return fc
	}
	return TextFormatCode
}

func (ci *ConnInfo) ResultFormatCodeForOID(oid uint32) int16 {
	fc, ok := ci.oidToResultFormatCode[oid]
	if ok {
		return fc
	}
	return TextFormatCode
}

// DeepCopy makes a deep copy of the ConnInfo.
func (ci *ConnInfo) DeepCopy() *ConnInfo {
	ci2 := &ConnInfo{
		oidToDataType:         make(map[uint32]*DataType, len(ci.oidToDataType)),
		nameToDataType:        make(map[string]*DataType, len(ci.nameToDataType)),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, len(ci.reflectTypeToDataType)),
	}

	for _, dt := range ci.oidToDataType {
		ci2.RegisterDataType(DataType{
			Value: reflect.New(reflect.ValueOf(dt.Value).Elem().Type()).Interface().(Value),
			Name:  dt.Name,
			OID:   dt.OID,
		})
	}

	return ci2
}

func (ci *ConnInfo) Scan(oid uint32, formatCode int16, buf []byte, dest interface{}) error {
	if dest, ok := dest.(BinaryDecoder); ok && formatCode == BinaryFormatCode {
		return dest.DecodeBinary(ci, buf)
	}

	if dest, ok := dest.(TextDecoder); ok && formatCode == TextFormatCode {
		return dest.DecodeText(ci, buf)
	}

	if dt, ok := ci.DataTypeForOID(oid); ok {
		value := dt.Value
		switch formatCode {
		case TextFormatCode:
			if textDecoder, ok := value.(TextDecoder); ok {
				err := textDecoder.DecodeText(ci, buf)
				if err != nil {
					return err
				}
			} else {
				return errors.Errorf("%T is not a pgtype.TextDecoder", value)
			}
		case BinaryFormatCode:
			if binaryDecoder, ok := value.(BinaryDecoder); ok {
				err := binaryDecoder.DecodeBinary(ci, buf)
				if err != nil {
					return err
				}
			} else {
				return errors.Errorf("%T is not a pgtype.BinaryDecoder", value)
			}
		default:
			return errors.Errorf("unknown format code: %v", formatCode)
		}

		if scanner, ok := dest.(sql.Scanner); ok {
			sqlSrc, err := DatabaseSQLValue(ci, value)
			if err != nil {
				return err
			}
			return scanner.Scan(sqlSrc)
		} else {
			return value.AssignTo(dest)
		}
	}

	return scanUnknownType(oid, formatCode, buf, dest)
}

func scanUnknownType(oid uint32, formatCode int16, buf []byte, dest interface{}) error {
	switch dest := dest.(type) {
	case *string:
		if formatCode == BinaryFormatCode {
			return errors.Errorf("unknown oid %d in binary format cannot be scanned into %t", oid, dest)
		}
		*dest = string(buf)
		return nil
	case *[]byte:
		*dest = buf
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dest); retry {
			return scanUnknownType(oid, formatCode, buf, nextDst)
		}
		return errors.Errorf("unknown oid %d cannot be scanned into %t", oid, dest)
	}
}

var nameValues map[string]Value

func init() {
	nameValues = map[string]Value{
		"_aclitem":     &ACLItemArray{},
		"_bool":        &BoolArray{},
		"_bpchar":      &BPCharArray{},
		"_bytea":       &ByteaArray{},
		"_cidr":        &CIDRArray{},
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
		"_uuid":        &UUIDArray{},
		"_varchar":     &VarcharArray{},
		"aclitem":      &ACLItem{},
		"bit":          &Bit{},
		"bool":         &Bool{},
		"box":          &Box{},
		"bpchar":       &BPChar{},
		"bytea":        &Bytea{},
		"char":         &QChar{},
		"cid":          &CID{},
		"cidr":         &CIDR{},
		"circle":       &Circle{},
		"date":         &Date{},
		"daterange":    &Daterange{},
		"float4":       &Float4{},
		"float8":       &Float8{},
		"hstore":       &Hstore{},
		"inet":         &Inet{},
		"int2":         &Int2{},
		"int4":         &Int4{},
		"int4range":    &Int4range{},
		"int8":         &Int8{},
		"int8range":    &Int8range{},
		"interval":     &Interval{},
		"json":         &JSON{},
		"jsonb":        &JSONB{},
		"line":         &Line{},
		"lseg":         &Lseg{},
		"macaddr":      &Macaddr{},
		"name":         &Name{},
		"numeric":      &Numeric{},
		"numrange":     &Numrange{},
		"oid":          &OIDValue{},
		"path":         &Path{},
		"point":        &Point{},
		"polygon":      &Polygon{},
		"record":       &Record{},
		"text":         &Text{},
		"tid":          &TID{},
		"timestamp":    &Timestamp{},
		"timestamptz":  &Timestamptz{},
		"tsrange":      &Tsrange{},
		"tstzrange":    &Tstzrange{},
		"unknown":      &Unknown{},
		"uuid":         &UUID{},
		"varbit":       &Varbit{},
		"varchar":      &Varchar{},
		"xid":          &XID{},
	}
}
