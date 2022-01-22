package pgtype

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"time"
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
	JSONArrayOID        = 199
	PointOID            = 600
	LsegOID             = 601
	PathOID             = 602
	BoxOID              = 603
	PolygonOID          = 604
	LineOID             = 628
	LineArrayOID        = 629
	CIDROID             = 650
	CIDRArrayOID        = 651
	Float4OID           = 700
	Float8OID           = 701
	CircleOID           = 718
	CircleArrayOID      = 719
	UnknownOID          = 705
	MacaddrOID          = 829
	InetOID             = 869
	BoolArrayOID        = 1000
	QCharArrayOID       = 1003
	NameArrayOID        = 1003
	Int2ArrayOID        = 1005
	Int4ArrayOID        = 1007
	TextArrayOID        = 1009
	TIDArrayOID         = 1010
	ByteaArrayOID       = 1001
	XIDArrayOID         = 1011
	CIDArrayOID         = 1012
	BPCharArrayOID      = 1014
	VarcharArrayOID     = 1015
	Int8ArrayOID        = 1016
	PointArrayOID       = 1017
	LsegArrayOID        = 1018
	PathArrayOID        = 1019
	BoxArrayOID         = 1020
	Float4ArrayOID      = 1021
	Float8ArrayOID      = 1022
	PolygonArrayOID     = 1027
	OIDArrayOID         = 1028
	ACLItemOID          = 1033
	ACLItemArrayOID     = 1034
	MacaddrArrayOID     = 1040
	InetArrayOID        = 1041
	BPCharOID           = 1042
	VarcharOID          = 1043
	DateOID             = 1082
	TimeOID             = 1083
	TimestampOID        = 1114
	TimestampArrayOID   = 1115
	DateArrayOID        = 1182
	TimeArrayOID        = 1183
	TimestamptzOID      = 1184
	TimestamptzArrayOID = 1185
	IntervalOID         = 1186
	IntervalArrayOID    = 1187
	NumericArrayOID     = 1231
	BitOID              = 1560
	BitArrayOID         = 1561
	VarbitOID           = 1562
	VarbitArrayOID      = 1563
	NumericOID          = 1700
	RecordOID           = 2249
	UUIDOID             = 2950
	UUIDArrayOID        = 2951
	JSONBOID            = 3802
	JSONBArrayOID       = 3807
	DaterangeOID        = 3912
	Int4rangeOID        = 3904
	NumrangeOID         = 3906
	TsrangeOID          = 3908
	TsrangeArrayOID     = 3909
	TstzrangeOID        = 3910
	TstzrangeArrayOID   = 3911
	Int8rangeOID        = 3926
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

type Codec interface {
	// FormatSupported returns true if the format is supported.
	FormatSupported(int16) bool

	// PreferredFormat returns the preferred format.
	PreferredFormat() int16

	// PlanEncode returns an Encode plan for encoding value into PostgreSQL format for oid and format. If no plan can be
	// found then nil is returned.
	PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan

	// PlanScan returns a ScanPlan for scanning a PostgreSQL value into a destination with the same type as target. If
	// actualTarget is true then the returned ScanPlan may be optimized to directly scan into target. If no plan can be
	// found then nil is returned.
	PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan

	// DecodeDatabaseSQLValue returns src decoded into a value compatible with the sql.Scanner interface.
	DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error)

	// DecodeValue returns src decoded into its default format.
	DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error)
}

type nullAssignmentError struct {
	dst interface{}
}

func (e *nullAssignmentError) Error() string {
	return fmt.Sprintf("cannot assign NULL to %T", e.dst)
}

type DataType struct {
	Codec Codec
	Name  string
	OID   uint32
}

type ConnInfo struct {
	oidToDataType         map[uint32]*DataType
	nameToDataType        map[string]*DataType
	reflectTypeToName     map[reflect.Type]string
	oidToFormatCode       map[uint32]int16
	oidToResultFormatCode map[uint32]int16

	reflectTypeToDataType map[reflect.Type]*DataType

	preferAssignToOverSQLScannerTypes map[reflect.Type]struct{}
}

func newConnInfo() *ConnInfo {
	return &ConnInfo{
		oidToDataType:                     make(map[uint32]*DataType),
		nameToDataType:                    make(map[string]*DataType),
		reflectTypeToName:                 make(map[reflect.Type]string),
		oidToFormatCode:                   make(map[uint32]int16),
		oidToResultFormatCode:             make(map[uint32]int16),
		preferAssignToOverSQLScannerTypes: make(map[reflect.Type]struct{}),
	}
}

func NewConnInfo() *ConnInfo {
	ci := newConnInfo()

	ci.RegisterDataType(DataType{Name: "_aclitem", OID: ACLItemArrayOID, Codec: &ArrayCodec{ElementCodec: &TextFormatOnlyCodec{TextCodec{}}, ElementOID: ACLItemOID}})
	ci.RegisterDataType(DataType{Name: "_bool", OID: BoolArrayOID, Codec: &ArrayCodec{ElementCodec: BoolCodec{}, ElementOID: BoolOID}})
	ci.RegisterDataType(DataType{Name: "_bpchar", OID: BPCharArrayOID, Codec: &ArrayCodec{ElementCodec: TextCodec{}, ElementOID: BPCharOID}})
	ci.RegisterDataType(DataType{Name: "_bytea", OID: ByteaArrayOID, Codec: &ArrayCodec{ElementCodec: ByteaCodec{}, ElementOID: ByteaOID}})
	ci.RegisterDataType(DataType{Name: "_cidr", OID: CIDRArrayOID, Codec: &ArrayCodec{ElementCodec: InetCodec{}, ElementOID: CIDROID}})
	ci.RegisterDataType(DataType{Name: "_date", OID: DateArrayOID, Codec: &ArrayCodec{ElementCodec: DateCodec{}, ElementOID: DateOID}})
	ci.RegisterDataType(DataType{Name: "_float4", OID: Float4ArrayOID, Codec: &ArrayCodec{ElementCodec: Float4Codec{}, ElementOID: Float4OID}})
	ci.RegisterDataType(DataType{Name: "_float8", OID: Float8ArrayOID, Codec: &ArrayCodec{ElementCodec: Float8Codec{}, ElementOID: Float8OID}})
	ci.RegisterDataType(DataType{Name: "_inet", OID: InetArrayOID, Codec: &ArrayCodec{ElementCodec: InetCodec{}, ElementOID: InetOID}})
	ci.RegisterDataType(DataType{Name: "_int2", OID: Int2ArrayOID, Codec: &ArrayCodec{ElementCodec: Int2Codec{}, ElementOID: Int2OID}})
	ci.RegisterDataType(DataType{Name: "_int4", OID: Int4ArrayOID, Codec: &ArrayCodec{ElementCodec: Int4Codec{}, ElementOID: Int4OID}})
	ci.RegisterDataType(DataType{Name: "_int8", OID: Int8ArrayOID, Codec: &ArrayCodec{ElementCodec: Int8Codec{}, ElementOID: Int8OID}})
	ci.RegisterDataType(DataType{Name: "_interval", OID: IntervalArrayOID, Codec: &ArrayCodec{ElementCodec: IntervalCodec{}, ElementOID: IntervalOID}})
	ci.RegisterDataType(DataType{Name: "_box", OID: BoxArrayOID, Codec: &ArrayCodec{ElementCodec: BoxCodec{}, ElementOID: BoxOID}})
	ci.RegisterDataType(DataType{Name: "_line", OID: LineArrayOID, Codec: &ArrayCodec{ElementCodec: LineCodec{}, ElementOID: LineOID}})
	ci.RegisterDataType(DataType{Name: "_lseg", OID: LsegArrayOID, Codec: &ArrayCodec{ElementCodec: LsegCodec{}, ElementOID: LsegOID}})
	ci.RegisterDataType(DataType{Name: "_path", OID: PathArrayOID, Codec: &ArrayCodec{ElementCodec: PathCodec{}, ElementOID: PathOID}})
	ci.RegisterDataType(DataType{Name: "_circle", OID: CircleArrayOID, Codec: &ArrayCodec{ElementCodec: CircleCodec{}, ElementOID: CircleOID}})
	ci.RegisterDataType(DataType{Name: "_point", OID: PointArrayOID, Codec: &ArrayCodec{ElementCodec: PointCodec{}, ElementOID: PointOID}})
	ci.RegisterDataType(DataType{Name: "_polygon", OID: PolygonArrayOID, Codec: &ArrayCodec{ElementCodec: PolygonCodec{}, ElementOID: PolygonOID}})
	ci.RegisterDataType(DataType{Name: "_name", OID: NameArrayOID, Codec: &ArrayCodec{ElementCodec: TextCodec{}, ElementOID: NameOID}})
	ci.RegisterDataType(DataType{Name: "_char", OID: QCharArrayOID, Codec: &ArrayCodec{ElementCodec: QCharCodec{}, ElementOID: QCharOID}})
	ci.RegisterDataType(DataType{Name: "_numeric", OID: NumericArrayOID, Codec: &ArrayCodec{ElementCodec: NumericCodec{}, ElementOID: NumericOID}})
	ci.RegisterDataType(DataType{Name: "_text", OID: TextArrayOID, Codec: &ArrayCodec{ElementCodec: TextCodec{}, ElementOID: TextOID}})
	ci.RegisterDataType(DataType{Name: "_timestamp", OID: TimestampArrayOID, Codec: &ArrayCodec{ElementCodec: TimestampCodec{}, ElementOID: TimestampOID}})
	ci.RegisterDataType(DataType{Name: "_timestamptz", OID: TimestamptzArrayOID, Codec: &ArrayCodec{ElementCodec: TimestamptzCodec{}, ElementOID: TimestamptzOID}})
	ci.RegisterDataType(DataType{Name: "_macaddr", OID: MacaddrArrayOID, Codec: &ArrayCodec{ElementCodec: MacaddrCodec{}, ElementOID: MacaddrOID}})
	ci.RegisterDataType(DataType{Name: "_tid", OID: TIDArrayOID, Codec: &ArrayCodec{ElementCodec: TIDCodec{}, ElementOID: TIDOID}})
	ci.RegisterDataType(DataType{Name: "_uuid", OID: UUIDArrayOID, Codec: &ArrayCodec{ElementCodec: UUIDCodec{}, ElementOID: UUIDOID}})
	ci.RegisterDataType(DataType{Name: "_jsonb", OID: JSONBArrayOID, Codec: &ArrayCodec{ElementCodec: JSONBCodec{}, ElementOID: JSONBOID}})
	ci.RegisterDataType(DataType{Name: "_json", OID: JSONArrayOID, Codec: &ArrayCodec{ElementCodec: JSONCodec{}, ElementOID: JSONOID}})
	ci.RegisterDataType(DataType{Name: "_varchar", OID: VarcharArrayOID, Codec: &ArrayCodec{ElementCodec: TextCodec{}, ElementOID: VarcharOID}})
	ci.RegisterDataType(DataType{Name: "_bit", OID: BitArrayOID, Codec: &ArrayCodec{ElementCodec: BitsCodec{}, ElementOID: BitOID}})
	ci.RegisterDataType(DataType{Name: "_varbit", OID: VarbitArrayOID, Codec: &ArrayCodec{ElementCodec: BitsCodec{}, ElementOID: VarbitOID}})
	ci.RegisterDataType(DataType{Name: "_cid", OID: CIDArrayOID, Codec: &ArrayCodec{ElementCodec: Uint32Codec{}, ElementOID: CIDOID}})
	ci.RegisterDataType(DataType{Name: "_oid", OID: OIDArrayOID, Codec: &ArrayCodec{ElementCodec: Uint32Codec{}, ElementOID: OIDOID}})
	ci.RegisterDataType(DataType{Name: "_xid", OID: XIDArrayOID, Codec: &ArrayCodec{ElementCodec: Uint32Codec{}, ElementOID: XIDOID}})
	ci.RegisterDataType(DataType{Name: "_time", OID: TimeArrayOID, Codec: &ArrayCodec{ElementCodec: TimeCodec{}, ElementOID: TimeOID}})
	ci.RegisterDataType(DataType{Name: "aclitem", OID: ACLItemOID, Codec: &TextFormatOnlyCodec{TextCodec{}}})
	ci.RegisterDataType(DataType{Name: "bit", OID: BitOID, Codec: BitsCodec{}})
	ci.RegisterDataType(DataType{Name: "bool", OID: BoolOID, Codec: BoolCodec{}})
	ci.RegisterDataType(DataType{Name: "box", OID: BoxOID, Codec: BoxCodec{}})
	ci.RegisterDataType(DataType{Name: "bpchar", OID: BPCharOID, Codec: TextCodec{}})
	ci.RegisterDataType(DataType{Name: "bytea", OID: ByteaOID, Codec: ByteaCodec{}})
	ci.RegisterDataType(DataType{Name: "char", OID: QCharOID, Codec: QCharCodec{}})
	ci.RegisterDataType(DataType{Name: "cid", OID: CIDOID, Codec: Uint32Codec{}})
	ci.RegisterDataType(DataType{Name: "cidr", OID: CIDROID, Codec: InetCodec{}})
	ci.RegisterDataType(DataType{Name: "circle", OID: CircleOID, Codec: CircleCodec{}})
	ci.RegisterDataType(DataType{Name: "date", OID: DateOID, Codec: DateCodec{}})
	// ci.RegisterDataType(DataType{Value: &Daterange{}, Name: "daterange", OID: DaterangeOID})
	ci.RegisterDataType(DataType{Name: "float4", OID: Float4OID, Codec: Float4Codec{}})
	ci.RegisterDataType(DataType{Name: "float8", OID: Float8OID, Codec: Float8Codec{}})
	ci.RegisterDataType(DataType{Name: "inet", OID: InetOID, Codec: InetCodec{}})
	ci.RegisterDataType(DataType{Name: "int2", OID: Int2OID, Codec: Int2Codec{}})
	ci.RegisterDataType(DataType{Name: "int4", OID: Int4OID, Codec: Int4Codec{}})
	// ci.RegisterDataType(DataType{Value: &Int4range{}, Name: "int4range", OID: Int4rangeOID})
	ci.RegisterDataType(DataType{Name: "int8", OID: Int8OID, Codec: Int8Codec{}})
	// ci.RegisterDataType(DataType{Value: &Int8range{}, Name: "int8range", OID: Int8rangeOID})
	ci.RegisterDataType(DataType{Name: "interval", OID: IntervalOID, Codec: IntervalCodec{}})
	ci.RegisterDataType(DataType{Name: "json", OID: JSONOID, Codec: JSONCodec{}})
	ci.RegisterDataType(DataType{Name: "jsonb", OID: JSONBOID, Codec: JSONBCodec{}})
	ci.RegisterDataType(DataType{Name: "line", OID: LineOID, Codec: LineCodec{}})
	ci.RegisterDataType(DataType{Name: "lseg", OID: LsegOID, Codec: LsegCodec{}})
	ci.RegisterDataType(DataType{Name: "macaddr", OID: MacaddrOID, Codec: MacaddrCodec{}})
	ci.RegisterDataType(DataType{Name: "name", OID: NameOID, Codec: TextCodec{}})
	ci.RegisterDataType(DataType{Name: "numeric", OID: NumericOID, Codec: NumericCodec{}})
	// ci.RegisterDataType(DataType{Value: &Numrange{}, Name: "numrange", OID: NumrangeOID})
	ci.RegisterDataType(DataType{Name: "oid", OID: OIDOID, Codec: Uint32Codec{}})
	ci.RegisterDataType(DataType{Name: "path", OID: PathOID, Codec: PathCodec{}})
	ci.RegisterDataType(DataType{Name: "point", OID: PointOID, Codec: PointCodec{}})
	ci.RegisterDataType(DataType{Name: "polygon", OID: PolygonOID, Codec: PolygonCodec{}})
	// ci.RegisterDataType(DataType{Value: &Record{}, Name: "record", OID: RecordOID})
	ci.RegisterDataType(DataType{Name: "text", OID: TextOID, Codec: TextCodec{}})
	ci.RegisterDataType(DataType{Name: "tid", OID: TIDOID, Codec: TIDCodec{}})
	ci.RegisterDataType(DataType{Name: "time", OID: TimeOID, Codec: TimeCodec{}})
	ci.RegisterDataType(DataType{Name: "timestamp", OID: TimestampOID, Codec: TimestampCodec{}})
	ci.RegisterDataType(DataType{Name: "timestamptz", OID: TimestamptzOID, Codec: TimestamptzCodec{}})
	// ci.RegisterDataType(DataType{Value: &Tsrange{}, Name: "tsrange", OID: TsrangeOID})
	// ci.RegisterDataType(DataType{Value: &TsrangeArray{}, Name: "_tsrange", OID: TsrangeArrayOID})
	// ci.RegisterDataType(DataType{Value: &Tstzrange{}, Name: "tstzrange", OID: TstzrangeOID})
	// ci.RegisterDataType(DataType{Value: &TstzrangeArray{}, Name: "_tstzrange", OID: TstzrangeArrayOID})
	ci.RegisterDataType(DataType{Name: "unknown", OID: UnknownOID, Codec: TextCodec{}})
	ci.RegisterDataType(DataType{Name: "uuid", OID: UUIDOID, Codec: UUIDCodec{}})
	ci.RegisterDataType(DataType{Name: "varbit", OID: VarbitOID, Codec: BitsCodec{}})
	ci.RegisterDataType(DataType{Name: "varchar", OID: VarcharOID, Codec: TextCodec{}})
	ci.RegisterDataType(DataType{Name: "xid", OID: XIDOID, Codec: Uint32Codec{}})

	registerDefaultPgTypeVariants := func(name, arrayName string, value interface{}) {
		// T
		ci.RegisterDefaultPgType(value, name)

		// *T
		valueType := reflect.TypeOf(value)
		ci.RegisterDefaultPgType(reflect.New(valueType).Interface(), name)

		// []T
		sliceType := reflect.SliceOf(valueType)
		ci.RegisterDefaultPgType(reflect.MakeSlice(sliceType, 0, 0).Interface(), arrayName)

		// *[]T
		ci.RegisterDefaultPgType(reflect.New(sliceType).Interface(), arrayName)

		// []*T
		sliceOfPointerType := reflect.SliceOf(reflect.TypeOf(reflect.New(valueType).Interface()))
		ci.RegisterDefaultPgType(reflect.MakeSlice(sliceOfPointerType, 0, 0).Interface(), arrayName)

		// *[]*T
		ci.RegisterDefaultPgType(reflect.New(sliceOfPointerType).Interface(), arrayName)
	}

	// Integer types that directly map to a PostgreSQL type
	registerDefaultPgTypeVariants("int2", "_int2", int16(0))
	registerDefaultPgTypeVariants("int4", "_int4", int32(0))
	registerDefaultPgTypeVariants("int8", "_int8", int64(0))

	// Integer types that do not have a direct match to a PostgreSQL type
	registerDefaultPgTypeVariants("int8", "_int8", uint16(0))
	registerDefaultPgTypeVariants("int8", "_int8", uint32(0))
	registerDefaultPgTypeVariants("int8", "_int8", uint64(0))
	registerDefaultPgTypeVariants("int8", "_int8", int(0))
	registerDefaultPgTypeVariants("int8", "_int8", uint(0))

	registerDefaultPgTypeVariants("float4", "_float4", float32(0))
	registerDefaultPgTypeVariants("float8", "_float8", float64(0))

	registerDefaultPgTypeVariants("bool", "_bool", false)
	registerDefaultPgTypeVariants("timestamptz", "_timestamptz", time.Time{})
	registerDefaultPgTypeVariants("text", "_text", "")
	registerDefaultPgTypeVariants("bytea", "_bytea", []byte(nil))

	registerDefaultPgTypeVariants("inet", "_inet", net.IP{})
	registerDefaultPgTypeVariants("cidr", "_cidr", net.IPNet{})

	return ci
}

func (ci *ConnInfo) RegisterDataType(t DataType) {
	ci.oidToDataType[t.OID] = &t
	ci.nameToDataType[t.Name] = &t
	ci.oidToFormatCode[t.OID] = t.Codec.PreferredFormat()
	ci.reflectTypeToDataType = nil // Invalidated by type registration
}

// RegisterDefaultPgType registers a mapping of a Go type to a PostgreSQL type name. Typically the data type to be
// encoded or decoded is determined by the PostgreSQL OID. But if the OID of a value to be encoded or decoded is
// unknown, this additional mapping will be used by DataTypeForValue to determine a suitable data type.
func (ci *ConnInfo) RegisterDefaultPgType(value interface{}, name string) {
	ci.reflectTypeToName[reflect.TypeOf(value)] = name
	ci.reflectTypeToDataType = nil // Invalidated by registering a default type
}

func (ci *ConnInfo) DataTypeForOID(oid uint32) (*DataType, bool) {
	dt, ok := ci.oidToDataType[oid]
	return dt, ok
}

func (ci *ConnInfo) DataTypeForName(name string) (*DataType, bool) {
	dt, ok := ci.nameToDataType[name]
	return dt, ok
}

func (ci *ConnInfo) buildReflectTypeToDataType() {
	ci.reflectTypeToDataType = make(map[reflect.Type]*DataType)

	for reflectType, name := range ci.reflectTypeToName {
		if dt, ok := ci.nameToDataType[name]; ok {
			ci.reflectTypeToDataType[reflectType] = dt
		}
	}
}

// DataTypeForValue finds a data type suitable for v. Use RegisterDataType to register types that can encode and decode
// themselves. Use RegisterDefaultPgType to register that can be handled by a registered data type.
func (ci *ConnInfo) DataTypeForValue(v interface{}) (*DataType, bool) {
	if ci.reflectTypeToDataType == nil {
		ci.buildReflectTypeToDataType()
	}

	dt, ok := ci.reflectTypeToDataType[reflect.TypeOf(v)]
	return dt, ok
}

func (ci *ConnInfo) FormatCodeForOID(oid uint32) int16 {
	fc, ok := ci.oidToFormatCode[oid]
	if ok {
		return fc
	}
	return TextFormatCode
}

// PreferAssignToOverSQLScannerForType makes a sql.Scanner type use the AssignTo scan path instead of sql.Scanner.
// This is primarily for efficient integration with 3rd party numeric and UUID types.
func (ci *ConnInfo) PreferAssignToOverSQLScannerForType(value interface{}) {
	ci.preferAssignToOverSQLScannerTypes[reflect.TypeOf(value)] = struct{}{}
}

// EncodePlan is a precompiled plan to encode a particular type into a particular OID and format.
type EncodePlan interface {
	// Encode appends the encoded bytes of value to buf. If value is the SQL value NULL then append nothing and return
	// (nil, nil). The caller of Encode is responsible for writing the correct NULL value or the length of the data
	// written.
	Encode(value interface{}, buf []byte) (newBuf []byte, err error)
}

// ScanPlan is a precompiled plan to scan into a type of destination.
type ScanPlan interface {
	// Scan scans src into dst. If the dst type has changed in an incompatible way a ScanPlan should automatically
	// replan and scan.
	Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error
}

type scanPlanDstResultDecoder struct{}

func (scanPlanDstResultDecoder) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	newPlan := ci.PlanScan(oid, formatCode, dst)
	return newPlan.Scan(ci, oid, formatCode, src, dst)
}

type scanPlanCodecSQLScanner struct{ c Codec }

func (plan *scanPlanCodecSQLScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	value, err := plan.c.DecodeDatabaseSQLValue(ci, oid, formatCode, src)
	if err != nil {
		return err
	}

	scanner := dst.(sql.Scanner)
	return scanner.Scan(value)
}

type scanPlanSQLScanner struct{}

func (scanPlanSQLScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	scanner := dst.(sql.Scanner)
	if src == nil {
		// This is necessary because interface value []byte:nil does not equal nil:nil for the binary format path and the
		// text format path would be converted to empty string.
		return scanner.Scan(nil)
	} else if formatCode == BinaryFormatCode {
		return scanner.Scan(src)
	} else {
		return scanner.Scan(string(src))
	}
}

type scanPlanReflection struct{}

func (scanPlanReflection) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	// We might be given a pointer to something that implements the decoder interface(s),
	// even though the pointer itself doesn't.
	refVal := reflect.ValueOf(dst)
	if refVal.Kind() == reflect.Ptr && refVal.Type().Elem().Kind() == reflect.Ptr {
		// If the database returned NULL, then we set dest as nil to indicate that.
		if src == nil {
			nilPtr := reflect.Zero(refVal.Type().Elem())
			refVal.Elem().Set(nilPtr)
			return nil
		}

		// We need to allocate an element, and set the destination to it
		// Then we can retry as that element.
		elemPtr := reflect.New(refVal.Type().Elem().Elem())
		refVal.Elem().Set(elemPtr)

		plan := ci.PlanScan(oid, formatCode, elemPtr.Interface())
		return plan.Scan(ci, oid, formatCode, src, elemPtr.Interface())
	}

	return scanUnknownType(oid, formatCode, src, dst)
}

type scanPlanBinaryInt64 struct{}

func (scanPlanBinaryInt64) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for int8: %v", len(src))
	}

	if p, ok := (dst).(*int64); ok {
		*p = int64(binary.BigEndian.Uint64(src))
		return nil
	}

	newPlan := ci.PlanScan(oid, formatCode, dst)
	return newPlan.Scan(ci, oid, formatCode, src, dst)
}

type scanPlanBinaryFloat32 struct{}

func (scanPlanBinaryFloat32) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length for int4: %v", len(src))
	}

	if p, ok := (dst).(*float32); ok {
		n := int32(binary.BigEndian.Uint32(src))
		*p = float32(math.Float32frombits(uint32(n)))
		return nil
	}

	newPlan := ci.PlanScan(oid, formatCode, dst)
	return newPlan.Scan(ci, oid, formatCode, src, dst)
}

type scanPlanBinaryFloat64 struct{}

func (scanPlanBinaryFloat64) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for int8: %v", len(src))
	}

	if p, ok := (dst).(*float64); ok {
		n := int64(binary.BigEndian.Uint64(src))
		*p = float64(math.Float64frombits(uint64(n)))
		return nil
	}

	newPlan := ci.PlanScan(oid, formatCode, dst)
	return newPlan.Scan(ci, oid, formatCode, src, dst)
}

type scanPlanBinaryBytes struct{}

func (scanPlanBinaryBytes) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if p, ok := (dst).(*[]byte); ok {
		*p = src
		return nil
	}

	newPlan := ci.PlanScan(oid, formatCode, dst)
	return newPlan.Scan(ci, oid, formatCode, src, dst)
}

type scanPlanString struct{}

func (scanPlanString) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if p, ok := (dst).(*string); ok {
		*p = string(src)
		return nil
	}

	newPlan := ci.PlanScan(oid, formatCode, dst)
	return newPlan.Scan(ci, oid, formatCode, src, dst)
}

type tryWrapScanPlanFunc func(dst interface{}) (plan WrappedScanPlanNextSetter, nextDst interface{}, ok bool)

type pointerPointerScanPlan struct {
	dstType reflect.Type
	next    ScanPlan
}

func (plan *pointerPointerScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *pointerPointerScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if plan.dstType != reflect.TypeOf(dst) {
		newPlan := ci.PlanScan(oid, formatCode, dst)
		return newPlan.Scan(ci, oid, formatCode, src, dst)
	}

	el := reflect.ValueOf(dst).Elem()
	if src == nil {
		el.Set(reflect.Zero(el.Type()))
		return nil
	}

	el.Set(reflect.New(el.Type().Elem()))
	return plan.next.Scan(ci, oid, formatCode, src, el.Interface())
}

func tryPointerPointerScanPlan(dst interface{}) (plan WrappedScanPlanNextSetter, nextDst interface{}, ok bool) {
	if dstValue := reflect.ValueOf(dst); dstValue.Kind() == reflect.Ptr {
		elemValue := dstValue.Elem()
		if elemValue.Kind() == reflect.Ptr {
			plan = &pointerPointerScanPlan{dstType: dstValue.Type()}
			return plan, reflect.Zero(elemValue.Type()).Interface(), true
		}
	}

	return nil, nil, false
}

// SkipUnderlyingTypePlanner prevents PlanScan and PlanDecode from trying to use the underlying type.
type SkipUnderlyingTypePlanner interface {
	SkipUnderlyingTypePlan()
}

var elemKindToPointerTypes map[reflect.Kind]reflect.Type = map[reflect.Kind]reflect.Type{
	reflect.Int:     reflect.TypeOf(new(int)),
	reflect.Int8:    reflect.TypeOf(new(int8)),
	reflect.Int16:   reflect.TypeOf(new(int16)),
	reflect.Int32:   reflect.TypeOf(new(int32)),
	reflect.Int64:   reflect.TypeOf(new(int64)),
	reflect.Uint:    reflect.TypeOf(new(uint)),
	reflect.Uint8:   reflect.TypeOf(new(uint8)),
	reflect.Uint16:  reflect.TypeOf(new(uint16)),
	reflect.Uint32:  reflect.TypeOf(new(uint32)),
	reflect.Uint64:  reflect.TypeOf(new(uint64)),
	reflect.Float32: reflect.TypeOf(new(float32)),
	reflect.Float64: reflect.TypeOf(new(float64)),
	reflect.String:  reflect.TypeOf(new(string)),
}

type underlyingTypeScanPlan struct {
	dstType     reflect.Type
	nextDstType reflect.Type
	next        ScanPlan
}

func (plan *underlyingTypeScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *underlyingTypeScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if plan.dstType != reflect.TypeOf(dst) {
		newPlan := ci.PlanScan(oid, formatCode, dst)
		return newPlan.Scan(ci, oid, formatCode, src, dst)
	}

	return plan.next.Scan(ci, oid, formatCode, src, reflect.ValueOf(dst).Convert(plan.nextDstType).Interface())
}

func tryUnderlyingTypeScanPlan(dst interface{}) (plan WrappedScanPlanNextSetter, nextDst interface{}, ok bool) {
	if _, ok := dst.(SkipUnderlyingTypePlanner); ok {
		return nil, nil, false
	}

	dstValue := reflect.ValueOf(dst)

	if dstValue.Kind() == reflect.Ptr {
		elemValue := dstValue.Elem()
		nextDstType := elemKindToPointerTypes[elemValue.Kind()]
		if nextDstType != nil && dstValue.Type() != nextDstType {
			return &underlyingTypeScanPlan{dstType: dstValue.Type(), nextDstType: nextDstType}, dstValue.Convert(nextDstType).Interface(), true
		}
	}

	return nil, nil, false
}

type WrappedScanPlanNextSetter interface {
	SetNext(ScanPlan)
	ScanPlan
}

func tryWrapBuiltinTypeScanPlan(dst interface{}) (plan WrappedScanPlanNextSetter, nextDst interface{}, ok bool) {
	switch dst := dst.(type) {
	case *int8:
		return &wrapInt8ScanPlan{}, (*int8Wrapper)(dst), true
	case *int16:
		return &wrapInt16ScanPlan{}, (*int16Wrapper)(dst), true
	case *int32:
		return &wrapInt32ScanPlan{}, (*int32Wrapper)(dst), true
	case *int64:
		return &wrapInt64ScanPlan{}, (*int64Wrapper)(dst), true
	case *int:
		return &wrapIntScanPlan{}, (*intWrapper)(dst), true
	case *uint8:
		return &wrapUint8ScanPlan{}, (*uint8Wrapper)(dst), true
	case *uint16:
		return &wrapUint16ScanPlan{}, (*uint16Wrapper)(dst), true
	case *uint32:
		return &wrapUint32ScanPlan{}, (*uint32Wrapper)(dst), true
	case *uint64:
		return &wrapUint64ScanPlan{}, (*uint64Wrapper)(dst), true
	case *uint:
		return &wrapUintScanPlan{}, (*uintWrapper)(dst), true
	case *float32:
		return &wrapFloat32ScanPlan{}, (*float32Wrapper)(dst), true
	case *float64:
		return &wrapFloat64ScanPlan{}, (*float64Wrapper)(dst), true
	case *string:
		return &wrapStringScanPlan{}, (*stringWrapper)(dst), true
	case *time.Time:
		return &wrapTimeScanPlan{}, (*timeWrapper)(dst), true
	case *time.Duration:
		return &wrapDurationScanPlan{}, (*durationWrapper)(dst), true
	case *net.IPNet:
		return &wrapNetIPNetScanPlan{}, (*netIPNetWrapper)(dst), true
	case *net.IP:
		return &wrapNetIPScanPlan{}, (*netIPWrapper)(dst), true
	case *map[string]*string:
		return &wrapMapStringToPointerStringScanPlan{}, (*mapStringToPointerStringWrapper)(dst), true
	case *map[string]string:
		return &wrapMapStringToStringScanPlan{}, (*mapStringToStringWrapper)(dst), true
	case *[16]byte:
		return &wrapByte16ScanPlan{}, (*byte16Wrapper)(dst), true
	case *[]byte:
		return &wrapByteSliceScanPlan{}, (*byteSliceWrapper)(dst), true
	}

	return nil, nil, false
}

type wrapInt8ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt8ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt8ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*int8Wrapper)(dst.(*int8)))
}

type wrapInt16ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt16ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt16ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*int16Wrapper)(dst.(*int16)))
}

type wrapInt32ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt32ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt32ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*int32Wrapper)(dst.(*int32)))
}

type wrapInt64ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt64ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt64ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*int64Wrapper)(dst.(*int64)))
}

type wrapIntScanPlan struct {
	next ScanPlan
}

func (plan *wrapIntScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapIntScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*intWrapper)(dst.(*int)))
}

type wrapUint8ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint8ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint8ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*uint8Wrapper)(dst.(*uint8)))
}

type wrapUint16ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint16ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint16ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*uint16Wrapper)(dst.(*uint16)))
}

type wrapUint32ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint32ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint32ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*uint32Wrapper)(dst.(*uint32)))
}

type wrapUint64ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint64ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint64ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*uint64Wrapper)(dst.(*uint64)))
}

type wrapUintScanPlan struct {
	next ScanPlan
}

func (plan *wrapUintScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUintScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*uintWrapper)(dst.(*uint)))
}

type wrapFloat32ScanPlan struct {
	next ScanPlan
}

func (plan *wrapFloat32ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapFloat32ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*float32Wrapper)(dst.(*float32)))
}

type wrapFloat64ScanPlan struct {
	next ScanPlan
}

func (plan *wrapFloat64ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapFloat64ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*float64Wrapper)(dst.(*float64)))
}

type wrapStringScanPlan struct {
	next ScanPlan
}

func (plan *wrapStringScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapStringScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*stringWrapper)(dst.(*string)))
}

type wrapTimeScanPlan struct {
	next ScanPlan
}

func (plan *wrapTimeScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapTimeScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*timeWrapper)(dst.(*time.Time)))
}

type wrapDurationScanPlan struct {
	next ScanPlan
}

func (plan *wrapDurationScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapDurationScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*durationWrapper)(dst.(*time.Duration)))
}

type wrapNetIPNetScanPlan struct {
	next ScanPlan
}

func (plan *wrapNetIPNetScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapNetIPNetScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*netIPNetWrapper)(dst.(*net.IPNet)))
}

type wrapNetIPScanPlan struct {
	next ScanPlan
}

func (plan *wrapNetIPScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapNetIPScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*netIPWrapper)(dst.(*net.IP)))
}

type wrapMapStringToPointerStringScanPlan struct {
	next ScanPlan
}

func (plan *wrapMapStringToPointerStringScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapMapStringToPointerStringScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*mapStringToPointerStringWrapper)(dst.(*map[string]*string)))
}

type wrapMapStringToStringScanPlan struct {
	next ScanPlan
}

func (plan *wrapMapStringToStringScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapMapStringToStringScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*mapStringToStringWrapper)(dst.(*map[string]string)))
}

type wrapByte16ScanPlan struct {
	next ScanPlan
}

func (plan *wrapByte16ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapByte16ScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*byte16Wrapper)(dst.(*[16]byte)))
}

type wrapByteSliceScanPlan struct {
	next ScanPlan
}

func (plan *wrapByteSliceScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapByteSliceScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	return plan.next.Scan(ci, oid, formatCode, src, (*byteSliceWrapper)(dst.(*[]byte)))
}

type pointerEmptyInterfaceScanPlan struct {
	codec Codec
}

func (plan *pointerEmptyInterfaceScanPlan) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	value, err := plan.codec.DecodeValue(ci, oid, formatCode, src)
	if err != nil {
		return err
	}

	ptrAny := dst.(*interface{})
	*ptrAny = value

	return nil
}

// PlanScan prepares a plan to scan a value into dst.
func (ci *ConnInfo) PlanScan(oid uint32, formatCode int16, dst interface{}) ScanPlan {
	if _, ok := dst.(*UndecodedBytes); ok {
		return scanPlanAnyToUndecodedBytes{}
	}

	switch formatCode {
	case BinaryFormatCode:
		switch dst.(type) {
		case *string:
			switch oid {
			case TextOID, VarcharOID:
				return scanPlanString{}
			}
		case *int64:
			if oid == Int8OID {
				return scanPlanBinaryInt64{}
			}
		case *float32:
			if oid == Float4OID {
				return scanPlanBinaryFloat32{}
			}
		case *float64:
			if oid == Float8OID {
				return scanPlanBinaryFloat64{}
			}
		case *[]byte:
			switch oid {
			case ByteaOID, TextOID, VarcharOID, JSONOID:
				return scanPlanBinaryBytes{}
			}
		}
	case TextFormatCode:
		switch dst.(type) {
		case *string:
			return scanPlanString{}
		case *[]byte:
			if oid != ByteaOID {
				return scanPlanBinaryBytes{}
			}
		case TextScanner:
			return scanPlanTextAnyToTextScanner{}
		}
	}

	var dt *DataType

	if oid == 0 {
		if dataType, ok := ci.DataTypeForValue(dst); ok {
			dt = dataType
			oid = dt.OID // Preserve assumed OID in case we are recursively called below.
		}
	} else {
		if dataType, ok := ci.DataTypeForOID(oid); ok {
			dt = dataType
		}
	}

	if dt != nil {
		if plan := dt.Codec.PlanScan(ci, oid, formatCode, dst, false); plan != nil {
			return plan
		}

		tryWrappers := []tryWrapScanPlanFunc{
			tryPointerPointerScanPlan,
			tryUnderlyingTypeScanPlan,
			tryWrapBuiltinTypeScanPlan,
		}

		for _, f := range tryWrappers {
			if wrapperPlan, nextDst, ok := f(dst); ok {
				if nextPlan := ci.PlanScan(oid, formatCode, nextDst); nextPlan != nil {
					if _, ok := nextPlan.(scanPlanReflection); !ok { // avoid fallthrough -- this will go away when old system removed.
						wrapperPlan.SetNext(nextPlan)
						return wrapperPlan
					}
				}
			}
		}

		if _, ok := dst.(*interface{}); ok {
			return &pointerEmptyInterfaceScanPlan{codec: dt.Codec}
		}

		if _, ok := dst.(sql.Scanner); ok {
			return &scanPlanCodecSQLScanner{c: dt.Codec}
		}
	}

	if _, ok := dst.(sql.Scanner); ok {
		return scanPlanSQLScanner{}
	}

	return scanPlanReflection{}
}

func (ci *ConnInfo) Scan(oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if dst == nil {
		return nil
	}

	plan := ci.PlanScan(oid, formatCode, dst)
	return plan.Scan(ci, oid, formatCode, src, dst)
}

func scanUnknownType(oid uint32, formatCode int16, buf []byte, dest interface{}) error {
	switch dest := dest.(type) {
	case *string:
		if formatCode == BinaryFormatCode {
			return fmt.Errorf("unknown oid %d in binary format cannot be scanned into %T", oid, dest)
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
		return fmt.Errorf("unknown oid %d cannot be scanned into %T", oid, dest)
	}
}

var ErrScanTargetTypeChanged = errors.New("scan target type changed")

func codecScan(codec Codec, ci *ConnInfo, oid uint32, format int16, src []byte, dst interface{}) error {
	scanPlan := codec.PlanScan(ci, oid, format, dst, true)
	if scanPlan == nil {
		return fmt.Errorf("PlanScan did not find a plan")
	}
	return scanPlan.Scan(ci, oid, format, src, dst)
}

func codecDecodeToTextFormat(codec Codec, ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	if format == TextFormatCode {
		return string(src), nil
	} else {
		value, err := codec.DecodeValue(ci, oid, format, src)
		if err != nil {
			return nil, err
		}
		buf, err := ci.Encode(oid, TextFormatCode, value, nil)
		if err != nil {
			return nil, err
		}
		return string(buf), nil
	}
}

// PlanEncode returns an Encode plan for encoding value into PostgreSQL format for oid and format. If no plan can be
// found then nil is returned.
func (ci *ConnInfo) PlanEncode(oid uint32, format int16, value interface{}) EncodePlan {

	var dt *DataType

	if oid == 0 {
		if dataType, ok := ci.DataTypeForValue(value); ok {
			dt = dataType
			oid = dt.OID // Preserve assumed OID in case we are recursively called below.
		}
	} else {
		if dataType, ok := ci.DataTypeForOID(oid); ok {
			dt = dataType
		}
	}

	if dt != nil {
		if plan := dt.Codec.PlanEncode(ci, oid, format, value); plan != nil {
			return plan
		}

		tryWrappers := []tryWrapEncodePlanFunc{
			tryDerefPointerEncodePlan,
			tryUnderlyingTypeEncodePlan,
			tryWrapBuiltinTypeEncodePlan,
		}

		for _, f := range tryWrappers {
			if wrapperPlan, nextValue, ok := f(value); ok {
				if nextPlan := ci.PlanEncode(oid, format, nextValue); nextPlan != nil {
					wrapperPlan.SetNext(nextPlan)
					return wrapperPlan
				}
			}
		}
	}

	return nil
}

type tryWrapEncodePlanFunc func(value interface{}) (plan WrappedEncodePlanNextSetter, nextValue interface{}, ok bool)

type derefPointerEncodePlan struct {
	next EncodePlan
}

func (plan *derefPointerEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *derefPointerEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	ptr := reflect.ValueOf(value)

	if ptr.IsNil() {
		return nil, nil
	}

	return plan.next.Encode(ptr.Elem().Interface(), buf)
}

func tryDerefPointerEncodePlan(value interface{}) (plan WrappedEncodePlanNextSetter, nextValue interface{}, ok bool) {
	if valueType := reflect.TypeOf(value); valueType.Kind() == reflect.Ptr {
		return &derefPointerEncodePlan{}, reflect.New(valueType.Elem()).Elem().Interface(), true
	}

	return nil, nil, false
}

var kindToTypes map[reflect.Kind]reflect.Type = map[reflect.Kind]reflect.Type{
	reflect.Int:     reflect.TypeOf(int(0)),
	reflect.Int8:    reflect.TypeOf(int8(0)),
	reflect.Int16:   reflect.TypeOf(int16(0)),
	reflect.Int32:   reflect.TypeOf(int32(0)),
	reflect.Int64:   reflect.TypeOf(int64(0)),
	reflect.Uint:    reflect.TypeOf(uint(0)),
	reflect.Uint8:   reflect.TypeOf(uint8(0)),
	reflect.Uint16:  reflect.TypeOf(uint16(0)),
	reflect.Uint32:  reflect.TypeOf(uint32(0)),
	reflect.Uint64:  reflect.TypeOf(uint64(0)),
	reflect.Float32: reflect.TypeOf(float32(0)),
	reflect.Float64: reflect.TypeOf(float64(0)),
	reflect.String:  reflect.TypeOf(""),
}

type underlyingTypeEncodePlan struct {
	nextValueType reflect.Type
	next          EncodePlan
}

func (plan *underlyingTypeEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *underlyingTypeEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(reflect.ValueOf(value).Convert(plan.nextValueType).Interface(), buf)
}

func tryUnderlyingTypeEncodePlan(value interface{}) (plan WrappedEncodePlanNextSetter, nextValue interface{}, ok bool) {
	if _, ok := value.(SkipUnderlyingTypePlanner); ok {
		return nil, nil, false
	}

	refValue := reflect.ValueOf(value)

	nextValueType := kindToTypes[refValue.Kind()]
	if nextValueType != nil && refValue.Type() != nextValueType {
		return &underlyingTypeEncodePlan{nextValueType: nextValueType}, refValue.Convert(nextValueType).Interface(), true
	}

	return nil, nil, false
}

type WrappedEncodePlanNextSetter interface {
	SetNext(EncodePlan)
	EncodePlan
}

func tryWrapBuiltinTypeEncodePlan(value interface{}) (plan WrappedEncodePlanNextSetter, nextValue interface{}, ok bool) {
	switch value := value.(type) {
	case int8:
		return &wrapInt8EncodePlan{}, int8Wrapper(value), true
	case int16:
		return &wrapInt16EncodePlan{}, int16Wrapper(value), true
	case int32:
		return &wrapInt32EncodePlan{}, int32Wrapper(value), true
	case int64:
		return &wrapInt64EncodePlan{}, int64Wrapper(value), true
	case int:
		return &wrapIntEncodePlan{}, intWrapper(value), true
	case uint8:
		return &wrapUint8EncodePlan{}, uint8Wrapper(value), true
	case uint16:
		return &wrapUint16EncodePlan{}, uint16Wrapper(value), true
	case uint32:
		return &wrapUint32EncodePlan{}, uint32Wrapper(value), true
	case uint64:
		return &wrapUint64EncodePlan{}, uint64Wrapper(value), true
	case uint:
		return &wrapUintEncodePlan{}, uintWrapper(value), true
	case float32:
		return &wrapFloat32EncodePlan{}, float32Wrapper(value), true
	case float64:
		return &wrapFloat64EncodePlan{}, float64Wrapper(value), true
	case string:
		return &wrapStringEncodePlan{}, stringWrapper(value), true
	case time.Time:
		return &wrapTimeEncodePlan{}, timeWrapper(value), true
	case time.Duration:
		return &wrapDurationEncodePlan{}, durationWrapper(value), true
	case net.IPNet:
		return &wrapNetIPNetEncodePlan{}, netIPNetWrapper(value), true
	case net.IP:
		return &wrapNetIPEncodePlan{}, netIPWrapper(value), true
	case map[string]*string:
		return &wrapMapStringToPointerStringEncodePlan{}, mapStringToPointerStringWrapper(value), true
	case map[string]string:
		return &wrapMapStringToStringEncodePlan{}, mapStringToStringWrapper(value), true
	case [16]byte:
		return &wrapByte16EncodePlan{}, byte16Wrapper(value), true
	case []byte:
		return &wrapByteSliceEncodePlan{}, byteSliceWrapper(value), true
	case fmt.Stringer:
		return &wrapFmtStringerEncodePlan{}, fmtStringerWrapper{value}, true
	}

	return nil, nil, false
}

type wrapInt8EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt8EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt8EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int8Wrapper(value.(int8)), buf)
}

type wrapInt16EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt16EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt16EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int16Wrapper(value.(int16)), buf)
}

type wrapInt32EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt32EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt32EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int32Wrapper(value.(int32)), buf)
}

type wrapInt64EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt64EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt64EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int64Wrapper(value.(int64)), buf)
}

type wrapIntEncodePlan struct {
	next EncodePlan
}

func (plan *wrapIntEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapIntEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(intWrapper(value.(int)), buf)
}

type wrapUint8EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint8EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint8EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint8Wrapper(value.(uint8)), buf)
}

type wrapUint16EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint16EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint16EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint16Wrapper(value.(uint16)), buf)
}

type wrapUint32EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint32EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint32EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint32Wrapper(value.(uint32)), buf)
}

type wrapUint64EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint64EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint64EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint64Wrapper(value.(uint64)), buf)
}

type wrapUintEncodePlan struct {
	next EncodePlan
}

func (plan *wrapUintEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUintEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uintWrapper(value.(uint)), buf)
}

type wrapFloat32EncodePlan struct {
	next EncodePlan
}

func (plan *wrapFloat32EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapFloat32EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(float32Wrapper(value.(float32)), buf)
}

type wrapFloat64EncodePlan struct {
	next EncodePlan
}

func (plan *wrapFloat64EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapFloat64EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(float64Wrapper(value.(float64)), buf)
}

type wrapStringEncodePlan struct {
	next EncodePlan
}

func (plan *wrapStringEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapStringEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(stringWrapper(value.(string)), buf)
}

type wrapTimeEncodePlan struct {
	next EncodePlan
}

func (plan *wrapTimeEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapTimeEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(timeWrapper(value.(time.Time)), buf)
}

type wrapDurationEncodePlan struct {
	next EncodePlan
}

func (plan *wrapDurationEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapDurationEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(durationWrapper(value.(time.Duration)), buf)
}

type wrapNetIPNetEncodePlan struct {
	next EncodePlan
}

func (plan *wrapNetIPNetEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapNetIPNetEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(netIPNetWrapper(value.(net.IPNet)), buf)
}

type wrapNetIPEncodePlan struct {
	next EncodePlan
}

func (plan *wrapNetIPEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapNetIPEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(netIPWrapper(value.(net.IP)), buf)
}

type wrapMapStringToPointerStringEncodePlan struct {
	next EncodePlan
}

func (plan *wrapMapStringToPointerStringEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapMapStringToPointerStringEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(mapStringToPointerStringWrapper(value.(map[string]*string)), buf)
}

type wrapMapStringToStringEncodePlan struct {
	next EncodePlan
}

func (plan *wrapMapStringToStringEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapMapStringToStringEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(mapStringToStringWrapper(value.(map[string]string)), buf)
}

type wrapByte16EncodePlan struct {
	next EncodePlan
}

func (plan *wrapByte16EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapByte16EncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(byte16Wrapper(value.([16]byte)), buf)
}

type wrapByteSliceEncodePlan struct {
	next EncodePlan
}

func (plan *wrapByteSliceEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapByteSliceEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(byteSliceWrapper(value.([]byte)), buf)
}

type wrapFmtStringerEncodePlan struct {
	next EncodePlan
}

func (plan *wrapFmtStringerEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapFmtStringerEncodePlan) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(fmtStringerWrapper{value.(fmt.Stringer)}, buf)
}

// Encode appends the encoded bytes of value to buf. If value is the SQL value NULL then append nothing and return
// (nil, nil). The caller of Encode is responsible for writing the correct NULL value or the length of the data
// written.
func (ci *ConnInfo) Encode(oid uint32, formatCode int16, value interface{}, buf []byte) (newBuf []byte, err error) {
	if value == nil {
		return nil, nil
	}

	plan := ci.PlanEncode(oid, formatCode, value)
	if plan == nil {
		return nil, fmt.Errorf("unable to encode %v", value)
	}
	return plan.Encode(value, buf)
}
