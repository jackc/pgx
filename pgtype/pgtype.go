package pgtype

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"time"
)

// PostgreSQL oids for common types
const (
	BoolOID                = 16
	ByteaOID               = 17
	QCharOID               = 18
	NameOID                = 19
	Int8OID                = 20
	Int2OID                = 21
	Int4OID                = 23
	TextOID                = 25
	OIDOID                 = 26
	TIDOID                 = 27
	XIDOID                 = 28
	CIDOID                 = 29
	JSONOID                = 114
	JSONArrayOID           = 199
	PointOID               = 600
	LsegOID                = 601
	PathOID                = 602
	BoxOID                 = 603
	PolygonOID             = 604
	LineOID                = 628
	LineArrayOID           = 629
	CIDROID                = 650
	CIDRArrayOID           = 651
	Float4OID              = 700
	Float8OID              = 701
	CircleOID              = 718
	CircleArrayOID         = 719
	UnknownOID             = 705
	MacaddrOID             = 829
	InetOID                = 869
	BoolArrayOID           = 1000
	QCharArrayOID          = 1003
	NameArrayOID           = 1003
	Int2ArrayOID           = 1005
	Int4ArrayOID           = 1007
	TextArrayOID           = 1009
	TIDArrayOID            = 1010
	ByteaArrayOID          = 1001
	XIDArrayOID            = 1011
	CIDArrayOID            = 1012
	BPCharArrayOID         = 1014
	VarcharArrayOID        = 1015
	Int8ArrayOID           = 1016
	PointArrayOID          = 1017
	LsegArrayOID           = 1018
	PathArrayOID           = 1019
	BoxArrayOID            = 1020
	Float4ArrayOID         = 1021
	Float8ArrayOID         = 1022
	PolygonArrayOID        = 1027
	OIDArrayOID            = 1028
	ACLItemOID             = 1033
	ACLItemArrayOID        = 1034
	MacaddrArrayOID        = 1040
	InetArrayOID           = 1041
	BPCharOID              = 1042
	VarcharOID             = 1043
	DateOID                = 1082
	TimeOID                = 1083
	TimestampOID           = 1114
	TimestampArrayOID      = 1115
	DateArrayOID           = 1182
	TimeArrayOID           = 1183
	TimestamptzOID         = 1184
	TimestamptzArrayOID    = 1185
	IntervalOID            = 1186
	IntervalArrayOID       = 1187
	NumericArrayOID        = 1231
	BitOID                 = 1560
	BitArrayOID            = 1561
	VarbitOID              = 1562
	VarbitArrayOID         = 1563
	NumericOID             = 1700
	RecordOID              = 2249
	RecordArrayOID         = 2287
	UUIDOID                = 2950
	UUIDArrayOID           = 2951
	JSONBOID               = 3802
	JSONBArrayOID          = 3807
	DaterangeOID           = 3912
	DaterangeArrayOID      = 3913
	Int4rangeOID           = 3904
	Int4rangeArrayOID      = 3905
	NumrangeOID            = 3906
	NumrangeArrayOID       = 3907
	TsrangeOID             = 3908
	TsrangeArrayOID        = 3909
	TstzrangeOID           = 3910
	TstzrangeArrayOID      = 3911
	Int8rangeOID           = 3926
	Int8rangeArrayOID      = 3927
	Int4multirangeOID      = 4451
	NummultirangeOID       = 4532
	TsmultirangeOID        = 4533
	TstzmultirangeOID      = 4534
	DatemultirangeOID      = 4535
	Int8multirangeOID      = 4536
	Int4multirangeArrayOID = 6150
	NummultirangeArrayOID  = 6151
	TsmultirangeArrayOID   = 6152
	TstzmultirangeArrayOID = 6153
	DatemultirangeArrayOID = 6155
	Int8multirangeArrayOID = 6157
)

type InfinityModifier int8

const (
	Infinity         InfinityModifier = 1
	Finite           InfinityModifier = 0
	NegativeInfinity InfinityModifier = -Infinity
)

func (im InfinityModifier) String() string {
	switch im {
	case Finite:
		return "finite"
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

// A Codec converts between Go and PostgreSQL values.
type Codec interface {
	// FormatSupported returns true if the format is supported.
	FormatSupported(int16) bool

	// PreferredFormat returns the preferred format.
	PreferredFormat() int16

	// PlanEncode returns an EncodePlan for encoding value into PostgreSQL format for oid and format. If no plan can be
	// found then nil is returned.
	PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan

	// PlanScan returns a ScanPlan for scanning a PostgreSQL value into a destination with the same type as target. If
	// no plan can be found then nil is returned.
	PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan

	// DecodeDatabaseSQLValue returns src decoded into a value compatible with the sql.Scanner interface.
	DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error)

	// DecodeValue returns src decoded into its default format.
	DecodeValue(m *Map, oid uint32, format int16, src []byte) (any, error)
}

type nullAssignmentError struct {
	dst any
}

func (e *nullAssignmentError) Error() string {
	return fmt.Sprintf("cannot assign NULL to %T", e.dst)
}

type Type struct {
	Codec Codec
	Name  string
	OID   uint32
}

// Map is the mapping between PostgreSQL server types and Go type handling logic. It can encode values for
// transmission to a PostgreSQL server and scan received values.
type Map struct {
	oidToType         map[uint32]*Type
	nameToType        map[string]*Type
	reflectTypeToName map[reflect.Type]string
	oidToFormatCode   map[uint32]int16

	reflectTypeToType map[reflect.Type]*Type

	memoizedScanPlans   map[uint32]map[reflect.Type][2]ScanPlan
	memoizedEncodePlans map[uint32]map[reflect.Type][2]EncodePlan

	// TryWrapEncodePlanFuncs is a slice of functions that will wrap a value that cannot be encoded by the Codec. Every
	// time a wrapper is found the PlanEncode method will be recursively called with the new value. This allows several layers of wrappers
	// to be built up. There are default functions placed in this slice by NewMap(). In most cases these functions
	// should run last. i.e. Additional functions should typically be prepended not appended.
	TryWrapEncodePlanFuncs []TryWrapEncodePlanFunc

	// TryWrapScanPlanFuncs is a slice of functions that will wrap a target that cannot be scanned into by the Codec. Every
	// time a wrapper is found the PlanScan method will be recursively called with the new target. This allows several layers of wrappers
	// to be built up. There are default functions placed in this slice by NewMap(). In most cases these functions
	// should run last. i.e. Additional functions should typically be prepended not appended.
	TryWrapScanPlanFuncs []TryWrapScanPlanFunc
}

func NewMap() *Map {
	m := &Map{
		oidToType:         make(map[uint32]*Type),
		nameToType:        make(map[string]*Type),
		reflectTypeToName: make(map[reflect.Type]string),
		oidToFormatCode:   make(map[uint32]int16),

		memoizedScanPlans:   make(map[uint32]map[reflect.Type][2]ScanPlan),
		memoizedEncodePlans: make(map[uint32]map[reflect.Type][2]EncodePlan),

		TryWrapEncodePlanFuncs: []TryWrapEncodePlanFunc{
			TryWrapDerefPointerEncodePlan,
			TryWrapBuiltinTypeEncodePlan,
			TryWrapFindUnderlyingTypeEncodePlan,
			TryWrapStructEncodePlan,
			TryWrapSliceEncodePlan,
			TryWrapMultiDimSliceEncodePlan,
			TryWrapArrayEncodePlan,
		},

		TryWrapScanPlanFuncs: []TryWrapScanPlanFunc{
			TryPointerPointerScanPlan,
			TryWrapBuiltinTypeScanPlan,
			TryFindUnderlyingTypeScanPlan,
			TryWrapStructScanPlan,
			TryWrapPtrSliceScanPlan,
			TryWrapPtrMultiDimSliceScanPlan,
			TryWrapPtrArrayScanPlan,
		},
	}

	// Base types
	m.RegisterType(&Type{Name: "aclitem", OID: ACLItemOID, Codec: &TextFormatOnlyCodec{TextCodec{}}})
	m.RegisterType(&Type{Name: "bit", OID: BitOID, Codec: BitsCodec{}})
	m.RegisterType(&Type{Name: "bool", OID: BoolOID, Codec: BoolCodec{}})
	m.RegisterType(&Type{Name: "box", OID: BoxOID, Codec: BoxCodec{}})
	m.RegisterType(&Type{Name: "bpchar", OID: BPCharOID, Codec: TextCodec{}})
	m.RegisterType(&Type{Name: "bytea", OID: ByteaOID, Codec: ByteaCodec{}})
	m.RegisterType(&Type{Name: "char", OID: QCharOID, Codec: QCharCodec{}})
	m.RegisterType(&Type{Name: "cid", OID: CIDOID, Codec: Uint32Codec{}})
	m.RegisterType(&Type{Name: "cidr", OID: CIDROID, Codec: InetCodec{}})
	m.RegisterType(&Type{Name: "circle", OID: CircleOID, Codec: CircleCodec{}})
	m.RegisterType(&Type{Name: "date", OID: DateOID, Codec: DateCodec{}})
	m.RegisterType(&Type{Name: "float4", OID: Float4OID, Codec: Float4Codec{}})
	m.RegisterType(&Type{Name: "float8", OID: Float8OID, Codec: Float8Codec{}})
	m.RegisterType(&Type{Name: "inet", OID: InetOID, Codec: InetCodec{}})
	m.RegisterType(&Type{Name: "int2", OID: Int2OID, Codec: Int2Codec{}})
	m.RegisterType(&Type{Name: "int4", OID: Int4OID, Codec: Int4Codec{}})
	m.RegisterType(&Type{Name: "int8", OID: Int8OID, Codec: Int8Codec{}})
	m.RegisterType(&Type{Name: "interval", OID: IntervalOID, Codec: IntervalCodec{}})
	m.RegisterType(&Type{Name: "json", OID: JSONOID, Codec: JSONCodec{}})
	m.RegisterType(&Type{Name: "jsonb", OID: JSONBOID, Codec: JSONBCodec{}})
	m.RegisterType(&Type{Name: "line", OID: LineOID, Codec: LineCodec{}})
	m.RegisterType(&Type{Name: "lseg", OID: LsegOID, Codec: LsegCodec{}})
	m.RegisterType(&Type{Name: "macaddr", OID: MacaddrOID, Codec: MacaddrCodec{}})
	m.RegisterType(&Type{Name: "name", OID: NameOID, Codec: TextCodec{}})
	m.RegisterType(&Type{Name: "numeric", OID: NumericOID, Codec: NumericCodec{}})
	m.RegisterType(&Type{Name: "oid", OID: OIDOID, Codec: Uint32Codec{}})
	m.RegisterType(&Type{Name: "path", OID: PathOID, Codec: PathCodec{}})
	m.RegisterType(&Type{Name: "point", OID: PointOID, Codec: PointCodec{}})
	m.RegisterType(&Type{Name: "polygon", OID: PolygonOID, Codec: PolygonCodec{}})
	m.RegisterType(&Type{Name: "record", OID: RecordOID, Codec: RecordCodec{}})
	m.RegisterType(&Type{Name: "text", OID: TextOID, Codec: TextCodec{}})
	m.RegisterType(&Type{Name: "tid", OID: TIDOID, Codec: TIDCodec{}})
	m.RegisterType(&Type{Name: "time", OID: TimeOID, Codec: TimeCodec{}})
	m.RegisterType(&Type{Name: "timestamp", OID: TimestampOID, Codec: TimestampCodec{}})
	m.RegisterType(&Type{Name: "timestamptz", OID: TimestamptzOID, Codec: TimestamptzCodec{}})
	m.RegisterType(&Type{Name: "unknown", OID: UnknownOID, Codec: TextCodec{}})
	m.RegisterType(&Type{Name: "uuid", OID: UUIDOID, Codec: UUIDCodec{}})
	m.RegisterType(&Type{Name: "varbit", OID: VarbitOID, Codec: BitsCodec{}})
	m.RegisterType(&Type{Name: "varchar", OID: VarcharOID, Codec: TextCodec{}})
	m.RegisterType(&Type{Name: "xid", OID: XIDOID, Codec: Uint32Codec{}})

	// Range types
	m.RegisterType(&Type{Name: "daterange", OID: DaterangeOID, Codec: &RangeCodec{ElementType: m.oidToType[DateOID]}})
	m.RegisterType(&Type{Name: "int4range", OID: Int4rangeOID, Codec: &RangeCodec{ElementType: m.oidToType[Int4OID]}})
	m.RegisterType(&Type{Name: "int8range", OID: Int8rangeOID, Codec: &RangeCodec{ElementType: m.oidToType[Int8OID]}})
	m.RegisterType(&Type{Name: "numrange", OID: NumrangeOID, Codec: &RangeCodec{ElementType: m.oidToType[NumericOID]}})
	m.RegisterType(&Type{Name: "tsrange", OID: TsrangeOID, Codec: &RangeCodec{ElementType: m.oidToType[TimestampOID]}})
	m.RegisterType(&Type{Name: "tstzrange", OID: TstzrangeOID, Codec: &RangeCodec{ElementType: m.oidToType[TimestamptzOID]}})

	// Multirange types
	m.RegisterType(&Type{Name: "datemultirange", OID: DatemultirangeOID, Codec: &MultirangeCodec{ElementType: m.oidToType[DaterangeOID]}})
	m.RegisterType(&Type{Name: "int4multirange", OID: Int4multirangeOID, Codec: &MultirangeCodec{ElementType: m.oidToType[Int4rangeOID]}})
	m.RegisterType(&Type{Name: "int8multirange", OID: Int8multirangeOID, Codec: &MultirangeCodec{ElementType: m.oidToType[Int8rangeOID]}})
	m.RegisterType(&Type{Name: "nummultirange", OID: NummultirangeOID, Codec: &MultirangeCodec{ElementType: m.oidToType[NumrangeOID]}})
	m.RegisterType(&Type{Name: "tsmultirange", OID: TsmultirangeOID, Codec: &MultirangeCodec{ElementType: m.oidToType[TsrangeOID]}})
	m.RegisterType(&Type{Name: "tstzmultirange", OID: TstzmultirangeOID, Codec: &MultirangeCodec{ElementType: m.oidToType[TstzrangeOID]}})

	// Array types
	m.RegisterType(&Type{Name: "_aclitem", OID: ACLItemArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[ACLItemOID]}})
	m.RegisterType(&Type{Name: "_bit", OID: BitArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[BitOID]}})
	m.RegisterType(&Type{Name: "_bool", OID: BoolArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[BoolOID]}})
	m.RegisterType(&Type{Name: "_box", OID: BoxArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[BoxOID]}})
	m.RegisterType(&Type{Name: "_bpchar", OID: BPCharArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[BPCharOID]}})
	m.RegisterType(&Type{Name: "_bytea", OID: ByteaArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[ByteaOID]}})
	m.RegisterType(&Type{Name: "_char", OID: QCharArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[QCharOID]}})
	m.RegisterType(&Type{Name: "_cid", OID: CIDArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[CIDOID]}})
	m.RegisterType(&Type{Name: "_cidr", OID: CIDRArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[CIDROID]}})
	m.RegisterType(&Type{Name: "_circle", OID: CircleArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[CircleOID]}})
	m.RegisterType(&Type{Name: "_date", OID: DateArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[DateOID]}})
	m.RegisterType(&Type{Name: "_daterange", OID: DaterangeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[DaterangeOID]}})
	m.RegisterType(&Type{Name: "_float4", OID: Float4ArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Float4OID]}})
	m.RegisterType(&Type{Name: "_float8", OID: Float8ArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Float8OID]}})
	m.RegisterType(&Type{Name: "_inet", OID: InetArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[InetOID]}})
	m.RegisterType(&Type{Name: "_int2", OID: Int2ArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Int2OID]}})
	m.RegisterType(&Type{Name: "_int4", OID: Int4ArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Int4OID]}})
	m.RegisterType(&Type{Name: "_int4range", OID: Int4rangeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Int4rangeOID]}})
	m.RegisterType(&Type{Name: "_int8", OID: Int8ArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Int8OID]}})
	m.RegisterType(&Type{Name: "_int8range", OID: Int8rangeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[Int8rangeOID]}})
	m.RegisterType(&Type{Name: "_interval", OID: IntervalArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[IntervalOID]}})
	m.RegisterType(&Type{Name: "_json", OID: JSONArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[JSONOID]}})
	m.RegisterType(&Type{Name: "_jsonb", OID: JSONBArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[JSONBOID]}})
	m.RegisterType(&Type{Name: "_line", OID: LineArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[LineOID]}})
	m.RegisterType(&Type{Name: "_lseg", OID: LsegArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[LsegOID]}})
	m.RegisterType(&Type{Name: "_macaddr", OID: MacaddrArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[MacaddrOID]}})
	m.RegisterType(&Type{Name: "_name", OID: NameArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[NameOID]}})
	m.RegisterType(&Type{Name: "_numeric", OID: NumericArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[NumericOID]}})
	m.RegisterType(&Type{Name: "_numrange", OID: NumrangeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[NumrangeOID]}})
	m.RegisterType(&Type{Name: "_oid", OID: OIDArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[OIDOID]}})
	m.RegisterType(&Type{Name: "_path", OID: PathArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[PathOID]}})
	m.RegisterType(&Type{Name: "_point", OID: PointArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[PointOID]}})
	m.RegisterType(&Type{Name: "_polygon", OID: PolygonArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[PolygonOID]}})
	m.RegisterType(&Type{Name: "_record", OID: RecordArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[RecordOID]}})
	m.RegisterType(&Type{Name: "_text", OID: TextArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TextOID]}})
	m.RegisterType(&Type{Name: "_tid", OID: TIDArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TIDOID]}})
	m.RegisterType(&Type{Name: "_time", OID: TimeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TimeOID]}})
	m.RegisterType(&Type{Name: "_timestamp", OID: TimestampArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TimestampOID]}})
	m.RegisterType(&Type{Name: "_timestamptz", OID: TimestamptzArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TimestamptzOID]}})
	m.RegisterType(&Type{Name: "_tsrange", OID: TsrangeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TsrangeOID]}})
	m.RegisterType(&Type{Name: "_tstzrange", OID: TstzrangeArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[TstzrangeOID]}})
	m.RegisterType(&Type{Name: "_uuid", OID: UUIDArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[UUIDOID]}})
	m.RegisterType(&Type{Name: "_varbit", OID: VarbitArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[VarbitOID]}})
	m.RegisterType(&Type{Name: "_varchar", OID: VarcharArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[VarcharOID]}})
	m.RegisterType(&Type{Name: "_xid", OID: XIDArrayOID, Codec: &ArrayCodec{ElementType: m.oidToType[XIDOID]}})

	// Integer types that directly map to a PostgreSQL type
	registerDefaultPgTypeVariants[int16](m, "int2")
	registerDefaultPgTypeVariants[int32](m, "int4")
	registerDefaultPgTypeVariants[int64](m, "int8")

	// Integer types that do not have a direct match to a PostgreSQL type
	registerDefaultPgTypeVariants[int8](m, "int8")
	registerDefaultPgTypeVariants[int](m, "int8")
	registerDefaultPgTypeVariants[uint8](m, "int8")
	registerDefaultPgTypeVariants[uint16](m, "int8")
	registerDefaultPgTypeVariants[uint32](m, "int8")
	registerDefaultPgTypeVariants[uint64](m, "numeric")
	registerDefaultPgTypeVariants[uint](m, "numeric")

	registerDefaultPgTypeVariants[float32](m, "float4")
	registerDefaultPgTypeVariants[float64](m, "float8")

	registerDefaultPgTypeVariants[bool](m, "bool")
	registerDefaultPgTypeVariants[time.Time](m, "timestamptz")
	registerDefaultPgTypeVariants[time.Duration](m, "interval")
	registerDefaultPgTypeVariants[string](m, "text")
	registerDefaultPgTypeVariants[[]byte](m, "bytea")

	registerDefaultPgTypeVariants[net.IP](m, "inet")
	registerDefaultPgTypeVariants[net.IPNet](m, "cidr")
	registerDefaultPgTypeVariants[netip.Addr](m, "inet")
	registerDefaultPgTypeVariants[netip.Prefix](m, "cidr")

	// pgtype provided structs
	registerDefaultPgTypeVariants[Bits](m, "varbit")
	registerDefaultPgTypeVariants[Bool](m, "bool")
	registerDefaultPgTypeVariants[Box](m, "box")
	registerDefaultPgTypeVariants[Circle](m, "circle")
	registerDefaultPgTypeVariants[Date](m, "date")
	registerDefaultPgTypeVariants[Range[Date]](m, "daterange")
	registerDefaultPgTypeVariants[Multirange[Range[Date]]](m, "datemultirange")
	registerDefaultPgTypeVariants[Float4](m, "float4")
	registerDefaultPgTypeVariants[Float8](m, "float8")
	registerDefaultPgTypeVariants[Range[Float8]](m, "numrange")                  // There is no PostgreSQL builtin float8range so map it to numrange.
	registerDefaultPgTypeVariants[Multirange[Range[Float8]]](m, "nummultirange") // There is no PostgreSQL builtin float8multirange so map it to nummultirange.
	registerDefaultPgTypeVariants[Int2](m, "int2")
	registerDefaultPgTypeVariants[Int4](m, "int4")
	registerDefaultPgTypeVariants[Range[Int4]](m, "int4range")
	registerDefaultPgTypeVariants[Multirange[Range[Int4]]](m, "int4multirange")
	registerDefaultPgTypeVariants[Int8](m, "int8")
	registerDefaultPgTypeVariants[Range[Int8]](m, "int8range")
	registerDefaultPgTypeVariants[Multirange[Range[Int8]]](m, "int8multirange")
	registerDefaultPgTypeVariants[Interval](m, "interval")
	registerDefaultPgTypeVariants[Line](m, "line")
	registerDefaultPgTypeVariants[Lseg](m, "lseg")
	registerDefaultPgTypeVariants[Numeric](m, "numeric")
	registerDefaultPgTypeVariants[Range[Numeric]](m, "numrange")
	registerDefaultPgTypeVariants[Multirange[Range[Numeric]]](m, "nummultirange")
	registerDefaultPgTypeVariants[Path](m, "path")
	registerDefaultPgTypeVariants[Point](m, "point")
	registerDefaultPgTypeVariants[Polygon](m, "polygon")
	registerDefaultPgTypeVariants[TID](m, "tid")
	registerDefaultPgTypeVariants[Text](m, "text")
	registerDefaultPgTypeVariants[Time](m, "time")
	registerDefaultPgTypeVariants[Timestamp](m, "timestamp")
	registerDefaultPgTypeVariants[Timestamptz](m, "timestamptz")
	registerDefaultPgTypeVariants[Range[Timestamp]](m, "tsrange")
	registerDefaultPgTypeVariants[Multirange[Range[Timestamp]]](m, "tsmultirange")
	registerDefaultPgTypeVariants[Range[Timestamptz]](m, "tstzrange")
	registerDefaultPgTypeVariants[Multirange[Range[Timestamptz]]](m, "tstzmultirange")
	registerDefaultPgTypeVariants[UUID](m, "uuid")

	return m
}

func (m *Map) RegisterType(t *Type) {
	m.oidToType[t.OID] = t
	m.nameToType[t.Name] = t
	m.oidToFormatCode[t.OID] = t.Codec.PreferredFormat()

	// Invalidated by type registration
	m.reflectTypeToType = nil
	for k := range m.memoizedScanPlans {
		delete(m.memoizedScanPlans, k)
	}
	for k := range m.memoizedEncodePlans {
		delete(m.memoizedEncodePlans, k)
	}
}

// RegisterDefaultPgType registers a mapping of a Go type to a PostgreSQL type name. Typically the data type to be
// encoded or decoded is determined by the PostgreSQL OID. But if the OID of a value to be encoded or decoded is
// unknown, this additional mapping will be used by TypeForValue to determine a suitable data type.
func (m *Map) RegisterDefaultPgType(value any, name string) {
	m.reflectTypeToName[reflect.TypeOf(value)] = name

	// Invalidated by type registration
	m.reflectTypeToType = nil
	for k := range m.memoizedScanPlans {
		delete(m.memoizedScanPlans, k)
	}
	for k := range m.memoizedEncodePlans {
		delete(m.memoizedEncodePlans, k)
	}
}

func (m *Map) TypeForOID(oid uint32) (*Type, bool) {
	dt, ok := m.oidToType[oid]
	return dt, ok
}

func (m *Map) TypeForName(name string) (*Type, bool) {
	dt, ok := m.nameToType[name]
	return dt, ok
}

func (m *Map) buildReflectTypeToType() {
	m.reflectTypeToType = make(map[reflect.Type]*Type)

	for reflectType, name := range m.reflectTypeToName {
		if dt, ok := m.nameToType[name]; ok {
			m.reflectTypeToType[reflectType] = dt
		}
	}
}

// TypeForValue finds a data type suitable for v. Use RegisterType to register types that can encode and decode
// themselves. Use RegisterDefaultPgType to register that can be handled by a registered data type.
func (m *Map) TypeForValue(v any) (*Type, bool) {
	if m.reflectTypeToType == nil {
		m.buildReflectTypeToType()
	}

	dt, ok := m.reflectTypeToType[reflect.TypeOf(v)]
	return dt, ok
}

// FormatCodeForOID returns the preferred format code for type oid. If the type is not registered it returns the text
// format code.
func (m *Map) FormatCodeForOID(oid uint32) int16 {
	fc, ok := m.oidToFormatCode[oid]
	if ok {
		return fc
	}
	return TextFormatCode
}

// EncodePlan is a precompiled plan to encode a particular type into a particular OID and format.
type EncodePlan interface {
	// Encode appends the encoded bytes of value to buf. If value is the SQL value NULL then append nothing and return
	// (nil, nil). The caller of Encode is responsible for writing the correct NULL value or the length of the data
	// written.
	Encode(value any, buf []byte) (newBuf []byte, err error)
}

// ScanPlan is a precompiled plan to scan into a type of destination.
type ScanPlan interface {
	// Scan scans src into target. src is only valid during the call to Scan. The ScanPlan must not retain a reference to
	// src.
	Scan(src []byte, target any) error
}

type scanPlanCodecSQLScanner struct {
	c          Codec
	m          *Map
	oid        uint32
	formatCode int16
}

func (plan *scanPlanCodecSQLScanner) Scan(src []byte, dst any) error {
	value, err := plan.c.DecodeDatabaseSQLValue(plan.m, plan.oid, plan.formatCode, src)
	if err != nil {
		return err
	}

	scanner := dst.(sql.Scanner)
	return scanner.Scan(value)
}

type scanPlanSQLScanner struct {
	formatCode int16
}

func (plan *scanPlanSQLScanner) Scan(src []byte, dst any) error {
	scanner := dst.(sql.Scanner)
	if src == nil {
		// This is necessary because interface value []byte:nil does not equal nil:nil for the binary format path and the
		// text format path would be converted to empty string.
		return scanner.Scan(nil)
	} else if plan.formatCode == BinaryFormatCode {
		return scanner.Scan(src)
	} else {
		return scanner.Scan(string(src))
	}
}

type scanPlanString struct{}

func (scanPlanString) Scan(src []byte, dst any) error {
	if src == nil {
		return fmt.Errorf("cannot scan NULL into %T", dst)
	}

	p := (dst).(*string)
	*p = string(src)
	return nil
}

type scanPlanAnyTextToBytes struct{}

func (scanPlanAnyTextToBytes) Scan(src []byte, dst any) error {
	dstBuf := dst.(*[]byte)
	if src == nil {
		*dstBuf = nil
		return nil
	}

	*dstBuf = make([]byte, len(src))
	copy(*dstBuf, src)
	return nil
}

type scanPlanFail struct {
	m          *Map
	oid        uint32
	formatCode int16
}

func (plan *scanPlanFail) Scan(src []byte, dst any) error {
	// If src is NULL it might be possible to scan into dst even though it is the types are not compatible. While this
	// may seem to be a contrived case it can occur when selecting NULL directly. PostgreSQL assigns it the type of text.
	// It would be surprising to the caller to have to cast the NULL (e.g. `select null::int`). So try to figure out a
	// compatible data type for dst and scan with that.
	//
	// See https://github.com/jackc/pgx/issues/1326
	if src == nil {
		// As a horrible hack try all types to find anything that can scan into dst.
		for oid := range plan.m.oidToType {
			// using planScan instead of Scan or PlanScan to avoid polluting the planned scan cache.
			plan := plan.m.planScan(oid, plan.formatCode, dst)
			if _, ok := plan.(*scanPlanFail); !ok {
				return plan.Scan(src, dst)
			}
		}
	}

	var format string
	switch plan.formatCode {
	case TextFormatCode:
		format = "text"
	case BinaryFormatCode:
		format = "binary"
	default:
		format = fmt.Sprintf("unknown %d", plan.formatCode)
	}

	var dataTypeName string
	if t, ok := plan.m.oidToType[plan.oid]; ok {
		dataTypeName = t.Name
	} else {
		dataTypeName = "unknown type"
	}

	return fmt.Errorf("cannot scan %s (OID %d) in %v format into %T", dataTypeName, plan.oid, format, dst)
}

// TryWrapScanPlanFunc is a function that tries to create a wrapper plan for target. If successful it returns a plan
// that will convert the target passed to Scan and then call the next plan. nextTarget is target as it will be converted
// by plan. It must be used to find another suitable ScanPlan. When it is found SetNext must be called on plan for it
// to be usabled. ok indicates if a suitable wrapper was found.
type TryWrapScanPlanFunc func(target any) (plan WrappedScanPlanNextSetter, nextTarget any, ok bool)

type pointerPointerScanPlan struct {
	dstType reflect.Type
	next    ScanPlan
}

func (plan *pointerPointerScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *pointerPointerScanPlan) Scan(src []byte, dst any) error {
	el := reflect.ValueOf(dst).Elem()
	if src == nil {
		el.Set(reflect.Zero(el.Type()))
		return nil
	}

	el.Set(reflect.New(el.Type().Elem()))
	return plan.next.Scan(src, el.Interface())
}

// TryPointerPointerScanPlan handles a pointer to a pointer by setting the target to nil for SQL NULL and allocating and
// scanning for non-NULL.
func TryPointerPointerScanPlan(target any) (plan WrappedScanPlanNextSetter, nextTarget any, ok bool) {
	if dstValue := reflect.ValueOf(target); dstValue.Kind() == reflect.Ptr {
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

func (plan *underlyingTypeScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, reflect.ValueOf(dst).Convert(plan.nextDstType).Interface())
}

// TryFindUnderlyingTypeScanPlan tries to convert to a Go builtin type. e.g. If value was of type MyString and
// MyString was defined as a string then a wrapper plan would be returned that converts MyString to string.
func TryFindUnderlyingTypeScanPlan(dst any) (plan WrappedScanPlanNextSetter, nextDst any, ok bool) {
	if _, ok := dst.(SkipUnderlyingTypePlanner); ok {
		return nil, nil, false
	}

	dstValue := reflect.ValueOf(dst)

	if dstValue.Kind() == reflect.Ptr {
		var elemValue reflect.Value
		if dstValue.IsNil() {
			elemValue = reflect.New(dstValue.Type().Elem()).Elem()
		} else {
			elemValue = dstValue.Elem()
		}
		nextDstType := elemKindToPointerTypes[elemValue.Kind()]
		if nextDstType == nil && elemValue.Kind() == reflect.Slice {
			if elemValue.Type().Elem().Kind() == reflect.Uint8 {
				var v *[]byte
				nextDstType = reflect.TypeOf(v)
			}
		}

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

// TryWrapBuiltinTypeScanPlan tries to wrap a builtin type with a wrapper that provides additional methods. e.g. If
// value was of type int32 then a wrapper plan would be returned that converts target to a value that implements
// Int64Scanner.
func TryWrapBuiltinTypeScanPlan(target any) (plan WrappedScanPlanNextSetter, nextDst any, ok bool) {
	switch target := target.(type) {
	case *int8:
		return &wrapInt8ScanPlan{}, (*int8Wrapper)(target), true
	case *int16:
		return &wrapInt16ScanPlan{}, (*int16Wrapper)(target), true
	case *int32:
		return &wrapInt32ScanPlan{}, (*int32Wrapper)(target), true
	case *int64:
		return &wrapInt64ScanPlan{}, (*int64Wrapper)(target), true
	case *int:
		return &wrapIntScanPlan{}, (*intWrapper)(target), true
	case *uint8:
		return &wrapUint8ScanPlan{}, (*uint8Wrapper)(target), true
	case *uint16:
		return &wrapUint16ScanPlan{}, (*uint16Wrapper)(target), true
	case *uint32:
		return &wrapUint32ScanPlan{}, (*uint32Wrapper)(target), true
	case *uint64:
		return &wrapUint64ScanPlan{}, (*uint64Wrapper)(target), true
	case *uint:
		return &wrapUintScanPlan{}, (*uintWrapper)(target), true
	case *float32:
		return &wrapFloat32ScanPlan{}, (*float32Wrapper)(target), true
	case *float64:
		return &wrapFloat64ScanPlan{}, (*float64Wrapper)(target), true
	case *string:
		return &wrapStringScanPlan{}, (*stringWrapper)(target), true
	case *time.Time:
		return &wrapTimeScanPlan{}, (*timeWrapper)(target), true
	case *time.Duration:
		return &wrapDurationScanPlan{}, (*durationWrapper)(target), true
	case *net.IPNet:
		return &wrapNetIPNetScanPlan{}, (*netIPNetWrapper)(target), true
	case *net.IP:
		return &wrapNetIPScanPlan{}, (*netIPWrapper)(target), true
	case *netip.Prefix:
		return &wrapNetipPrefixScanPlan{}, (*netipPrefixWrapper)(target), true
	case *netip.Addr:
		return &wrapNetipAddrScanPlan{}, (*netipAddrWrapper)(target), true
	case *map[string]*string:
		return &wrapMapStringToPointerStringScanPlan{}, (*mapStringToPointerStringWrapper)(target), true
	case *map[string]string:
		return &wrapMapStringToStringScanPlan{}, (*mapStringToStringWrapper)(target), true
	case *[16]byte:
		return &wrapByte16ScanPlan{}, (*byte16Wrapper)(target), true
	case *[]byte:
		return &wrapByteSliceScanPlan{}, (*byteSliceWrapper)(target), true
	}

	return nil, nil, false
}

type wrapInt8ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt8ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt8ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*int8Wrapper)(dst.(*int8)))
}

type wrapInt16ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt16ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt16ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*int16Wrapper)(dst.(*int16)))
}

type wrapInt32ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt32ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt32ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*int32Wrapper)(dst.(*int32)))
}

type wrapInt64ScanPlan struct {
	next ScanPlan
}

func (plan *wrapInt64ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapInt64ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*int64Wrapper)(dst.(*int64)))
}

type wrapIntScanPlan struct {
	next ScanPlan
}

func (plan *wrapIntScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapIntScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*intWrapper)(dst.(*int)))
}

type wrapUint8ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint8ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint8ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*uint8Wrapper)(dst.(*uint8)))
}

type wrapUint16ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint16ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint16ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*uint16Wrapper)(dst.(*uint16)))
}

type wrapUint32ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint32ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint32ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*uint32Wrapper)(dst.(*uint32)))
}

type wrapUint64ScanPlan struct {
	next ScanPlan
}

func (plan *wrapUint64ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUint64ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*uint64Wrapper)(dst.(*uint64)))
}

type wrapUintScanPlan struct {
	next ScanPlan
}

func (plan *wrapUintScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapUintScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*uintWrapper)(dst.(*uint)))
}

type wrapFloat32ScanPlan struct {
	next ScanPlan
}

func (plan *wrapFloat32ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapFloat32ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*float32Wrapper)(dst.(*float32)))
}

type wrapFloat64ScanPlan struct {
	next ScanPlan
}

func (plan *wrapFloat64ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapFloat64ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*float64Wrapper)(dst.(*float64)))
}

type wrapStringScanPlan struct {
	next ScanPlan
}

func (plan *wrapStringScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapStringScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*stringWrapper)(dst.(*string)))
}

type wrapTimeScanPlan struct {
	next ScanPlan
}

func (plan *wrapTimeScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapTimeScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*timeWrapper)(dst.(*time.Time)))
}

type wrapDurationScanPlan struct {
	next ScanPlan
}

func (plan *wrapDurationScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapDurationScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*durationWrapper)(dst.(*time.Duration)))
}

type wrapNetIPNetScanPlan struct {
	next ScanPlan
}

func (plan *wrapNetIPNetScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapNetIPNetScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*netIPNetWrapper)(dst.(*net.IPNet)))
}

type wrapNetIPScanPlan struct {
	next ScanPlan
}

func (plan *wrapNetIPScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapNetIPScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*netIPWrapper)(dst.(*net.IP)))
}

type wrapNetipPrefixScanPlan struct {
	next ScanPlan
}

func (plan *wrapNetipPrefixScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapNetipPrefixScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*netipPrefixWrapper)(dst.(*netip.Prefix)))
}

type wrapNetipAddrScanPlan struct {
	next ScanPlan
}

func (plan *wrapNetipAddrScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapNetipAddrScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*netipAddrWrapper)(dst.(*netip.Addr)))
}

type wrapMapStringToPointerStringScanPlan struct {
	next ScanPlan
}

func (plan *wrapMapStringToPointerStringScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapMapStringToPointerStringScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*mapStringToPointerStringWrapper)(dst.(*map[string]*string)))
}

type wrapMapStringToStringScanPlan struct {
	next ScanPlan
}

func (plan *wrapMapStringToStringScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapMapStringToStringScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*mapStringToStringWrapper)(dst.(*map[string]string)))
}

type wrapByte16ScanPlan struct {
	next ScanPlan
}

func (plan *wrapByte16ScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapByte16ScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*byte16Wrapper)(dst.(*[16]byte)))
}

type wrapByteSliceScanPlan struct {
	next ScanPlan
}

func (plan *wrapByteSliceScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapByteSliceScanPlan) Scan(src []byte, dst any) error {
	return plan.next.Scan(src, (*byteSliceWrapper)(dst.(*[]byte)))
}

type pointerEmptyInterfaceScanPlan struct {
	codec      Codec
	m          *Map
	oid        uint32
	formatCode int16
}

func (plan *pointerEmptyInterfaceScanPlan) Scan(src []byte, dst any) error {
	value, err := plan.codec.DecodeValue(plan.m, plan.oid, plan.formatCode, src)
	if err != nil {
		return err
	}

	ptrAny := dst.(*any)
	*ptrAny = value

	return nil
}

// TryWrapStructPlan tries to wrap a struct with a wrapper that implements CompositeIndexGetter.
func TryWrapStructScanPlan(target any) (plan WrappedScanPlanNextSetter, nextValue any, ok bool) {
	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr {
		return nil, nil, false
	}

	var targetElemValue reflect.Value
	if targetValue.IsNil() {
		targetElemValue = reflect.Zero(targetValue.Type().Elem())
	} else {
		targetElemValue = targetValue.Elem()
	}
	targetElemType := targetElemValue.Type()

	if targetElemType.Kind() == reflect.Struct {
		exportedFields := getExportedFieldValues(targetElemValue)
		if len(exportedFields) == 0 {
			return nil, nil, false
		}

		w := ptrStructWrapper{
			s:              target,
			exportedFields: exportedFields,
		}
		return &wrapAnyPtrStructScanPlan{}, &w, true
	}

	return nil, nil, false
}

type wrapAnyPtrStructScanPlan struct {
	next ScanPlan
}

func (plan *wrapAnyPtrStructScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapAnyPtrStructScanPlan) Scan(src []byte, target any) error {
	w := ptrStructWrapper{
		s:              target,
		exportedFields: getExportedFieldValues(reflect.ValueOf(target).Elem()),
	}

	return plan.next.Scan(src, &w)
}

// TryWrapPtrSliceScanPlan tries to wrap a pointer to a single dimension slice.
func TryWrapPtrSliceScanPlan(target any) (plan WrappedScanPlanNextSetter, nextValue any, ok bool) {
	// Avoid using reflect path for common types.
	switch target := target.(type) {
	case *[]int16:
		return &wrapPtrSliceScanPlan[int16]{}, (*FlatArray[int16])(target), true
	case *[]int32:
		return &wrapPtrSliceScanPlan[int32]{}, (*FlatArray[int32])(target), true
	case *[]int64:
		return &wrapPtrSliceScanPlan[int64]{}, (*FlatArray[int64])(target), true
	case *[]float32:
		return &wrapPtrSliceScanPlan[float32]{}, (*FlatArray[float32])(target), true
	case *[]float64:
		return &wrapPtrSliceScanPlan[float64]{}, (*FlatArray[float64])(target), true
	case *[]string:
		return &wrapPtrSliceScanPlan[string]{}, (*FlatArray[string])(target), true
	case *[]time.Time:
		return &wrapPtrSliceScanPlan[time.Time]{}, (*FlatArray[time.Time])(target), true
	}

	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr {
		return nil, nil, false
	}

	targetElemValue := targetValue.Elem()

	if targetElemValue.Kind() == reflect.Slice {
		return &wrapPtrSliceReflectScanPlan{}, &anySliceArrayReflect{slice: targetElemValue}, true
	}
	return nil, nil, false
}

type wrapPtrSliceScanPlan[T any] struct {
	next ScanPlan
}

func (plan *wrapPtrSliceScanPlan[T]) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapPtrSliceScanPlan[T]) Scan(src []byte, target any) error {
	return plan.next.Scan(src, (*FlatArray[T])(target.(*[]T)))
}

type wrapPtrSliceReflectScanPlan struct {
	next ScanPlan
}

func (plan *wrapPtrSliceReflectScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapPtrSliceReflectScanPlan) Scan(src []byte, target any) error {
	return plan.next.Scan(src, &anySliceArrayReflect{slice: reflect.ValueOf(target).Elem()})
}

// TryWrapPtrMultiDimSliceScanPlan tries to wrap a pointer to a multi-dimension slice.
func TryWrapPtrMultiDimSliceScanPlan(target any) (plan WrappedScanPlanNextSetter, nextValue any, ok bool) {
	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr {
		return nil, nil, false
	}

	targetElemValue := targetValue.Elem()

	if targetElemValue.Kind() == reflect.Slice {
		elemElemKind := targetElemValue.Type().Elem().Kind()
		if elemElemKind == reflect.Slice {
			if !isRagged(targetElemValue) {
				return &wrapPtrMultiDimSliceScanPlan{}, &anyMultiDimSliceArray{slice: targetValue.Elem()}, true
			}
		}
	}

	return nil, nil, false
}

type wrapPtrMultiDimSliceScanPlan struct {
	next ScanPlan
}

func (plan *wrapPtrMultiDimSliceScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapPtrMultiDimSliceScanPlan) Scan(src []byte, target any) error {
	return plan.next.Scan(src, &anyMultiDimSliceArray{slice: reflect.ValueOf(target).Elem()})
}

// TryWrapPtrArrayScanPlan tries to wrap a pointer to a single dimension array.
func TryWrapPtrArrayScanPlan(target any) (plan WrappedScanPlanNextSetter, nextValue any, ok bool) {
	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr {
		return nil, nil, false
	}

	targetElemValue := targetValue.Elem()

	if targetElemValue.Kind() == reflect.Array {
		return &wrapPtrArrayReflectScanPlan{}, &anyArrayArrayReflect{array: targetElemValue}, true
	}
	return nil, nil, false
}

type wrapPtrArrayReflectScanPlan struct {
	next ScanPlan
}

func (plan *wrapPtrArrayReflectScanPlan) SetNext(next ScanPlan) { plan.next = next }

func (plan *wrapPtrArrayReflectScanPlan) Scan(src []byte, target any) error {
	return plan.next.Scan(src, &anyArrayArrayReflect{array: reflect.ValueOf(target).Elem()})
}

// PlanScan prepares a plan to scan a value into target.
func (m *Map) PlanScan(oid uint32, formatCode int16, target any) ScanPlan {
	oidMemo := m.memoizedScanPlans[oid]
	if oidMemo == nil {
		oidMemo = make(map[reflect.Type][2]ScanPlan)
		m.memoizedScanPlans[oid] = oidMemo
	}
	targetReflectType := reflect.TypeOf(target)
	typeMemo := oidMemo[targetReflectType]
	plan := typeMemo[formatCode]
	if plan == nil {
		plan = m.planScan(oid, formatCode, target)
		typeMemo[formatCode] = plan
		oidMemo[targetReflectType] = typeMemo
	}

	return plan
}

func (m *Map) planScan(oid uint32, formatCode int16, target any) ScanPlan {
	if _, ok := target.(*UndecodedBytes); ok {
		return scanPlanAnyToUndecodedBytes{}
	}

	switch formatCode {
	case BinaryFormatCode:
		switch target.(type) {
		case *string:
			switch oid {
			case TextOID, VarcharOID:
				return scanPlanString{}
			}
		}
	case TextFormatCode:
		switch target.(type) {
		case *string:
			return scanPlanString{}
		case *[]byte:
			if oid != ByteaOID {
				return scanPlanAnyTextToBytes{}
			}
		case TextScanner:
			return scanPlanTextAnyToTextScanner{}
		}
	}

	var dt *Type

	if dataType, ok := m.TypeForOID(oid); ok {
		dt = dataType
	} else if dataType, ok := m.TypeForValue(target); ok {
		dt = dataType
		oid = dt.OID // Preserve assumed OID in case we are recursively called below.
	}

	if dt != nil {
		if plan := dt.Codec.PlanScan(m, oid, formatCode, target); plan != nil {
			return plan
		}
	}

	// This needs to happen before trying m.TryWrapScanPlanFuncs. Otherwise, a sql.Scanner would not get called if it was
	// defined on a type that could be unwrapped such as `type myString string`.
	//
	//  https://github.com/jackc/pgtype/issues/197
	if dt == nil {
		if _, ok := target.(sql.Scanner); ok {
			return &scanPlanSQLScanner{formatCode: formatCode}
		}
	}

	for _, f := range m.TryWrapScanPlanFuncs {
		if wrapperPlan, nextDst, ok := f(target); ok {
			if nextPlan := m.planScan(oid, formatCode, nextDst); nextPlan != nil {
				if _, failed := nextPlan.(*scanPlanFail); !failed {
					wrapperPlan.SetNext(nextPlan)
					return wrapperPlan
				}
			}
		}
	}

	if dt != nil {
		if _, ok := target.(*any); ok {
			return &pointerEmptyInterfaceScanPlan{codec: dt.Codec, m: m, oid: oid, formatCode: formatCode}
		}

		if _, ok := target.(sql.Scanner); ok {
			return &scanPlanCodecSQLScanner{c: dt.Codec, m: m, oid: oid, formatCode: formatCode}
		}
	}

	return &scanPlanFail{m: m, oid: oid, formatCode: formatCode}
}

func (m *Map) Scan(oid uint32, formatCode int16, src []byte, dst any) error {
	if dst == nil {
		return nil
	}

	plan := m.PlanScan(oid, formatCode, dst)
	return plan.Scan(src, dst)
}

func scanUnknownType(oid uint32, formatCode int16, buf []byte, dest any) error {
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

func codecScan(codec Codec, m *Map, oid uint32, format int16, src []byte, dst any) error {
	scanPlan := codec.PlanScan(m, oid, format, dst)
	if scanPlan == nil {
		return fmt.Errorf("PlanScan did not find a plan")
	}
	return scanPlan.Scan(src, dst)
}

func codecDecodeToTextFormat(codec Codec, m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	if format == TextFormatCode {
		return string(src), nil
	} else {
		value, err := codec.DecodeValue(m, oid, format, src)
		if err != nil {
			return nil, err
		}
		buf, err := m.Encode(oid, TextFormatCode, value, nil)
		if err != nil {
			return nil, err
		}
		return string(buf), nil
	}
}

// PlanEncode returns an Encode plan for encoding value into PostgreSQL format for oid and format. If no plan can be
// found then nil is returned.
func (m *Map) PlanEncode(oid uint32, format int16, value any) EncodePlan {
	oidMemo := m.memoizedEncodePlans[oid]
	if oidMemo == nil {
		oidMemo = make(map[reflect.Type][2]EncodePlan)
		m.memoizedEncodePlans[oid] = oidMemo
	}
	targetReflectType := reflect.TypeOf(value)
	typeMemo := oidMemo[targetReflectType]
	plan := typeMemo[format]
	if plan == nil {
		plan = m.planEncode(oid, format, value)
		typeMemo[format] = plan
		oidMemo[targetReflectType] = typeMemo
	}

	return plan
}

func (m *Map) planEncode(oid uint32, format int16, value any) EncodePlan {
	if format == TextFormatCode {
		switch value.(type) {
		case string:
			return encodePlanStringToAnyTextFormat{}
		case TextValuer:
			return encodePlanTextValuerToAnyTextFormat{}
		}
	}

	var dt *Type
	if dataType, ok := m.TypeForOID(oid); ok {
		dt = dataType
	} else {
		// If no type for the OID was found, then either it is unknowable (e.g. the simple protocol) or it is an
		// unregistered type. In either case try to find the type and OID that matches the value (e.g. a []byte would be
		// registered to PostgreSQL bytea).
		if dataType, ok := m.TypeForValue(value); ok {
			dt = dataType
			oid = dt.OID // Preserve assumed OID in case we are recursively called below.
		}
	}

	if dt != nil {
		if plan := dt.Codec.PlanEncode(m, oid, format, value); plan != nil {
			return plan
		}
	}

	for _, f := range m.TryWrapEncodePlanFuncs {
		if wrapperPlan, nextValue, ok := f(value); ok {
			if nextPlan := m.PlanEncode(oid, format, nextValue); nextPlan != nil {
				wrapperPlan.SetNext(nextPlan)
				return wrapperPlan
			}
		}
	}

	if _, ok := value.(driver.Valuer); ok {
		return &encodePlanDriverValuer{m: m, oid: oid, formatCode: format}
	}

	return nil
}

type encodePlanStringToAnyTextFormat struct{}

func (encodePlanStringToAnyTextFormat) Encode(value any, buf []byte) (newBuf []byte, err error) {
	s := value.(string)
	return append(buf, s...), nil
}

type encodePlanTextValuerToAnyTextFormat struct{}

func (encodePlanTextValuerToAnyTextFormat) Encode(value any, buf []byte) (newBuf []byte, err error) {
	t, err := value.(TextValuer).TextValue()
	if err != nil {
		return nil, err
	}
	if !t.Valid {
		return nil, nil
	}

	return append(buf, t.String...), nil
}

type encodePlanDriverValuer struct {
	m          *Map
	oid        uint32
	formatCode int16
}

func (plan *encodePlanDriverValuer) Encode(value any, buf []byte) (newBuf []byte, err error) {
	dv := value.(driver.Valuer)
	if dv == nil {
		return nil, nil
	}
	v, err := dv.Value()
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	newBuf, err = plan.m.Encode(plan.oid, plan.formatCode, v, buf)
	if err == nil {
		return newBuf, nil
	}

	s, ok := v.(string)
	if !ok {
		return nil, err
	}

	var scannedValue any
	scanErr := plan.m.Scan(plan.oid, TextFormatCode, []byte(s), &scannedValue)
	if scanErr != nil {
		return nil, err
	}

	// Prevent infinite loop. We can't encode this. See https://github.com/jackc/pgx/issues/1331.
	if reflect.TypeOf(value) == reflect.TypeOf(scannedValue) {
		return nil, fmt.Errorf("tried to encode %v via encoding to text and scanning but failed due to receiving same type back", value)
	}

	var err2 error
	newBuf, err2 = plan.m.Encode(plan.oid, BinaryFormatCode, scannedValue, buf)
	if err2 != nil {
		return nil, err
	}

	return newBuf, nil
}

// TryWrapEncodePlanFunc is a function that tries to create a wrapper plan for value. If successful it returns a plan
// that will convert the value passed to Encode and then call the next plan. nextValue is value as it will be converted
// by plan. It must be used to find another suitable EncodePlan. When it is found SetNext must be called on plan for it
// to be usabled. ok indicates if a suitable wrapper was found.
type TryWrapEncodePlanFunc func(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool)

type derefPointerEncodePlan struct {
	next EncodePlan
}

func (plan *derefPointerEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *derefPointerEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ptr := reflect.ValueOf(value)

	if ptr.IsNil() {
		return nil, nil
	}

	return plan.next.Encode(ptr.Elem().Interface(), buf)
}

// TryWrapDerefPointerEncodePlan tries to dereference a pointer. e.g. If value was of type *string then a wrapper plan
// would be returned that derefences the value.
func TryWrapDerefPointerEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

	if valueType := reflect.TypeOf(value); valueType != nil && valueType.Kind() == reflect.Ptr {
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

func (plan *underlyingTypeEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(reflect.ValueOf(value).Convert(plan.nextValueType).Interface(), buf)
}

// TryWrapFindUnderlyingTypeEncodePlan tries to convert to a Go builtin type. e.g. If value was of type MyString and
// MyString was defined as a string then a wrapper plan would be returned that converts MyString to string.
func TryWrapFindUnderlyingTypeEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

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

// TryWrapBuiltinTypeEncodePlan tries to wrap a builtin type with a wrapper that provides additional methods. e.g. If
// value was of type int32 then a wrapper plan would be returned that converts value to a type that implements
// Int64Valuer.
func TryWrapBuiltinTypeEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

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
	case netip.Prefix:
		return &wrapNetipPrefixEncodePlan{}, netipPrefixWrapper(value), true
	case netip.Addr:
		return &wrapNetipAddrEncodePlan{}, netipAddrWrapper(value), true
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

func (plan *wrapInt8EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int8Wrapper(value.(int8)), buf)
}

type wrapInt16EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt16EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt16EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int16Wrapper(value.(int16)), buf)
}

type wrapInt32EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt32EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt32EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int32Wrapper(value.(int32)), buf)
}

type wrapInt64EncodePlan struct {
	next EncodePlan
}

func (plan *wrapInt64EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapInt64EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(int64Wrapper(value.(int64)), buf)
}

type wrapIntEncodePlan struct {
	next EncodePlan
}

func (plan *wrapIntEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapIntEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(intWrapper(value.(int)), buf)
}

type wrapUint8EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint8EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint8EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint8Wrapper(value.(uint8)), buf)
}

type wrapUint16EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint16EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint16EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint16Wrapper(value.(uint16)), buf)
}

type wrapUint32EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint32EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint32EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint32Wrapper(value.(uint32)), buf)
}

type wrapUint64EncodePlan struct {
	next EncodePlan
}

func (plan *wrapUint64EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUint64EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uint64Wrapper(value.(uint64)), buf)
}

type wrapUintEncodePlan struct {
	next EncodePlan
}

func (plan *wrapUintEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapUintEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(uintWrapper(value.(uint)), buf)
}

type wrapFloat32EncodePlan struct {
	next EncodePlan
}

func (plan *wrapFloat32EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapFloat32EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(float32Wrapper(value.(float32)), buf)
}

type wrapFloat64EncodePlan struct {
	next EncodePlan
}

func (plan *wrapFloat64EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapFloat64EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(float64Wrapper(value.(float64)), buf)
}

type wrapStringEncodePlan struct {
	next EncodePlan
}

func (plan *wrapStringEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapStringEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(stringWrapper(value.(string)), buf)
}

type wrapTimeEncodePlan struct {
	next EncodePlan
}

func (plan *wrapTimeEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapTimeEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(timeWrapper(value.(time.Time)), buf)
}

type wrapDurationEncodePlan struct {
	next EncodePlan
}

func (plan *wrapDurationEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapDurationEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(durationWrapper(value.(time.Duration)), buf)
}

type wrapNetIPNetEncodePlan struct {
	next EncodePlan
}

func (plan *wrapNetIPNetEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapNetIPNetEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(netIPNetWrapper(value.(net.IPNet)), buf)
}

type wrapNetIPEncodePlan struct {
	next EncodePlan
}

func (plan *wrapNetIPEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapNetIPEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(netIPWrapper(value.(net.IP)), buf)
}

type wrapNetipPrefixEncodePlan struct {
	next EncodePlan
}

func (plan *wrapNetipPrefixEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapNetipPrefixEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(netipPrefixWrapper(value.(netip.Prefix)), buf)
}

type wrapNetipAddrEncodePlan struct {
	next EncodePlan
}

func (plan *wrapNetipAddrEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapNetipAddrEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(netipAddrWrapper(value.(netip.Addr)), buf)
}

type wrapMapStringToPointerStringEncodePlan struct {
	next EncodePlan
}

func (plan *wrapMapStringToPointerStringEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapMapStringToPointerStringEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(mapStringToPointerStringWrapper(value.(map[string]*string)), buf)
}

type wrapMapStringToStringEncodePlan struct {
	next EncodePlan
}

func (plan *wrapMapStringToStringEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapMapStringToStringEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(mapStringToStringWrapper(value.(map[string]string)), buf)
}

type wrapByte16EncodePlan struct {
	next EncodePlan
}

func (plan *wrapByte16EncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapByte16EncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(byte16Wrapper(value.([16]byte)), buf)
}

type wrapByteSliceEncodePlan struct {
	next EncodePlan
}

func (plan *wrapByteSliceEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapByteSliceEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(byteSliceWrapper(value.([]byte)), buf)
}

type wrapFmtStringerEncodePlan struct {
	next EncodePlan
}

func (plan *wrapFmtStringerEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapFmtStringerEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	return plan.next.Encode(fmtStringerWrapper{value.(fmt.Stringer)}, buf)
}

// TryWrapStructPlan tries to wrap a struct with a wrapper that implements CompositeIndexGetter.
func TryWrapStructEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

	if valueType := reflect.TypeOf(value); valueType != nil && valueType.Kind() == reflect.Struct {
		exportedFields := getExportedFieldValues(reflect.ValueOf(value))
		if len(exportedFields) == 0 {
			return nil, nil, false
		}

		w := structWrapper{
			s:              value,
			exportedFields: exportedFields,
		}
		return &wrapAnyStructEncodePlan{}, w, true
	}

	return nil, nil, false
}

type wrapAnyStructEncodePlan struct {
	next EncodePlan
}

func (plan *wrapAnyStructEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapAnyStructEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	w := structWrapper{
		s:              value,
		exportedFields: getExportedFieldValues(reflect.ValueOf(value)),
	}

	return plan.next.Encode(w, buf)
}

func getExportedFieldValues(structValue reflect.Value) []reflect.Value {
	structType := structValue.Type()
	exportedFields := make([]reflect.Value, 0, structValue.NumField())
	for i := 0; i < structType.NumField(); i++ {
		sf := structType.Field(i)
		if sf.IsExported() {
			exportedFields = append(exportedFields, structValue.Field(i))
		}
	}

	return exportedFields
}

func TryWrapSliceEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

	// Avoid using reflect path for common types.
	switch value := value.(type) {
	case []int16:
		return &wrapSliceEncodePlan[int16]{}, (FlatArray[int16])(value), true
	case []int32:
		return &wrapSliceEncodePlan[int32]{}, (FlatArray[int32])(value), true
	case []int64:
		return &wrapSliceEncodePlan[int64]{}, (FlatArray[int64])(value), true
	case []float32:
		return &wrapSliceEncodePlan[float32]{}, (FlatArray[float32])(value), true
	case []float64:
		return &wrapSliceEncodePlan[float64]{}, (FlatArray[float64])(value), true
	case []string:
		return &wrapSliceEncodePlan[string]{}, (FlatArray[string])(value), true
	case []time.Time:
		return &wrapSliceEncodePlan[time.Time]{}, (FlatArray[time.Time])(value), true
	}

	if valueType := reflect.TypeOf(value); valueType != nil && valueType.Kind() == reflect.Slice {
		w := anySliceArrayReflect{
			slice: reflect.ValueOf(value),
		}
		return &wrapSliceEncodeReflectPlan{}, w, true
	}

	return nil, nil, false
}

type wrapSliceEncodePlan[T any] struct {
	next EncodePlan
}

func (plan *wrapSliceEncodePlan[T]) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapSliceEncodePlan[T]) Encode(value any, buf []byte) (newBuf []byte, err error) {
	w := anySliceArrayReflect{
		slice: reflect.ValueOf(value),
	}

	return plan.next.Encode(w, buf)
}

type wrapSliceEncodeReflectPlan struct {
	next EncodePlan
}

func (plan *wrapSliceEncodeReflectPlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapSliceEncodeReflectPlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	w := anySliceArrayReflect{
		slice: reflect.ValueOf(value),
	}

	return plan.next.Encode(w, buf)
}

func TryWrapMultiDimSliceEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

	sliceValue := reflect.ValueOf(value)
	if sliceValue.Kind() == reflect.Slice {
		valueElemType := sliceValue.Type().Elem()

		if valueElemType.Kind() == reflect.Slice {
			if !isRagged(sliceValue) {
				w := anyMultiDimSliceArray{
					slice: reflect.ValueOf(value),
				}
				return &wrapMultiDimSliceEncodePlan{}, &w, true
			}
		}
	}

	return nil, nil, false
}

type wrapMultiDimSliceEncodePlan struct {
	next EncodePlan
}

func (plan *wrapMultiDimSliceEncodePlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapMultiDimSliceEncodePlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	w := anyMultiDimSliceArray{
		slice: reflect.ValueOf(value),
	}

	return plan.next.Encode(&w, buf)
}

func TryWrapArrayEncodePlan(value any) (plan WrappedEncodePlanNextSetter, nextValue any, ok bool) {
	if _, ok := value.(driver.Valuer); ok {
		return nil, nil, false
	}

	if valueType := reflect.TypeOf(value); valueType != nil && valueType.Kind() == reflect.Array {
		w := anyArrayArrayReflect{
			array: reflect.ValueOf(value),
		}
		return &wrapArrayEncodeReflectPlan{}, w, true
	}

	return nil, nil, false
}

type wrapArrayEncodeReflectPlan struct {
	next EncodePlan
}

func (plan *wrapArrayEncodeReflectPlan) SetNext(next EncodePlan) { plan.next = next }

func (plan *wrapArrayEncodeReflectPlan) Encode(value any, buf []byte) (newBuf []byte, err error) {
	w := anyArrayArrayReflect{
		array: reflect.ValueOf(value),
	}

	return plan.next.Encode(w, buf)
}

func newEncodeError(value any, m *Map, oid uint32, formatCode int16, err error) error {
	var format string
	switch formatCode {
	case TextFormatCode:
		format = "text"
	case BinaryFormatCode:
		format = "binary"
	default:
		format = fmt.Sprintf("unknown (%d)", formatCode)
	}

	var dataTypeName string
	if t, ok := m.oidToType[oid]; ok {
		dataTypeName = t.Name
	} else {
		dataTypeName = "unknown type"
	}

	return fmt.Errorf("unable to encode %#v into %s format for %s (OID %d): %s", value, format, dataTypeName, oid, err)
}

// Encode appends the encoded bytes of value to buf. If value is the SQL value NULL then append nothing and return
// (nil, nil). The caller of Encode is responsible for writing the correct NULL value or the length of the data
// written.
func (m *Map) Encode(oid uint32, formatCode int16, value any, buf []byte) (newBuf []byte, err error) {
	if value == nil {
		return nil, nil
	}

	plan := m.PlanEncode(oid, formatCode, value)
	if plan == nil {
		return nil, newEncodeError(value, m, oid, formatCode, errors.New("cannot find encode plan"))
	}

	newBuf, err = plan.Encode(value, buf)
	if err != nil {
		return nil, newEncodeError(value, m, oid, formatCode, err)
	}

	return newBuf, nil
}

// SQLScanner returns a database/sql.Scanner for v. This is necessary for types like Array[T] and Range[T] where the
// type needs assistance from Map to implement the sql.Scanner interface. It is not necessary for types like Box that
// implement sql.Scanner directly.
//
// This uses the type of v to look up the PostgreSQL OID that v presumably came from. This means v must be registered
// with m by calling RegisterDefaultPgType.
func (m *Map) SQLScanner(v any) sql.Scanner {
	if s, ok := v.(sql.Scanner); ok {
		return s
	}

	return &sqlScannerWrapper{m: m, v: v}
}

type sqlScannerWrapper struct {
	m *Map
	v any
}

func (w *sqlScannerWrapper) Scan(src any) error {
	t, ok := w.m.TypeForValue(w.v)
	if !ok {
		return fmt.Errorf("cannot convert to sql.Scanner: cannot find registered type for %T", w.v)
	}

	var bufSrc []byte
	if src != nil {
		switch src := src.(type) {
		case string:
			bufSrc = []byte(src)
		case []byte:
			bufSrc = src
		default:
			bufSrc = []byte(fmt.Sprint(bufSrc))
		}
	}

	return w.m.Scan(t.OID, TextFormatCode, bufSrc, w.v)
}
