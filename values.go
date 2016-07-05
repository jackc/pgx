package pgx

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL oids for common types
const (
	BoolOid             = 16
	ByteaOid            = 17
	Int8Oid             = 20
	Int2Oid             = 21
	Int4Oid             = 23
	TextOid             = 25
	OidOid              = 26
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
	InetArrayOid        = 1041
	VarcharOid          = 1043
	DateOid             = 1082
	TimestampOid        = 1114
	TimestampArrayOid   = 1115
	TimestampTzOid      = 1184
	TimestampTzArrayOid = 1185
	RecordOid           = 2249
	UuidOid             = 2950
	JsonbOid            = 3802
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)
const minInt = -maxInt - 1

// DefaultTypeFormats maps type names to their default requested format (text
// or binary). In theory the Scanner interface should be the one to determine
// the format of the returned values. However, the query has already been
// executed by the time Scan is called so it has no chance to set the format.
// So for types that should be returned in binary th
var DefaultTypeFormats map[string]int16

func init() {
	DefaultTypeFormats = map[string]int16{
		"_bool":        BinaryFormatCode,
		"_bytea":       BinaryFormatCode,
		"_cidr":        BinaryFormatCode,
		"_float4":      BinaryFormatCode,
		"_float8":      BinaryFormatCode,
		"_inet":        BinaryFormatCode,
		"_int2":        BinaryFormatCode,
		"_int4":        BinaryFormatCode,
		"_int8":        BinaryFormatCode,
		"_text":        BinaryFormatCode,
		"_timestamp":   BinaryFormatCode,
		"_timestamptz": BinaryFormatCode,
		"_varchar":     BinaryFormatCode,
		"bool":         BinaryFormatCode,
		"bytea":        BinaryFormatCode,
		"cidr":         BinaryFormatCode,
		"date":         BinaryFormatCode,
		"float4":       BinaryFormatCode,
		"float8":       BinaryFormatCode,
		"inet":         BinaryFormatCode,
		"int2":         BinaryFormatCode,
		"int4":         BinaryFormatCode,
		"int8":         BinaryFormatCode,
		"oid":          BinaryFormatCode,
		"record":       BinaryFormatCode,
		"text":         BinaryFormatCode,
		"timestamp":    BinaryFormatCode,
		"timestamptz":  BinaryFormatCode,
		"varchar":      BinaryFormatCode,
	}
}

// SerializationError occurs on failure to encode or decode a value
type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

// Scanner is an interface used to decode values from the PostgreSQL server.
type Scanner interface {
	// Scan MUST check r.Type().DataType (to check by OID) or
	// r.Type().DataTypeName (to check by name) to ensure that it is scanning an
	// expected column type. It also MUST check r.Type().FormatCode before
	// decoding. It should not assume that it was called on a data type or format
	// that it understands.
	Scan(r *ValueReader) error
}

// Encoder is an interface used to encode values for transmission to the
// PostgreSQL server.
type Encoder interface {
	// Encode writes the value to w.
	//
	// If the value is NULL an int32(-1) should be written.
	//
	// Encode MUST check oid to see if the parameter data type is compatible. If
	// this is not done, the PostgreSQL server may detect the error if the
	// expected data size or format of the encoded data does not match. But if
	// the encoded data is a valid representation of the data type PostgreSQL
	// expects such as date and int4, incorrect data may be stored.
	Encode(w *WriteBuf, oid Oid) error

	// FormatCode returns the format that the encoder writes the value. It must be
	// either pgx.TextFormatCode or pgx.BinaryFormatCode.
	FormatCode() int16
}

// NullFloat32 represents an float4 that may be null. NullFloat32 implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullFloat32 struct {
	Float32 float32
	Valid   bool // Valid is true if Float32 is not NULL
}

func (n *NullFloat32) Scan(vr *ValueReader) error {
	if vr.Type().DataType != Float4Oid {
		return SerializationError(fmt.Sprintf("NullFloat32.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Float32, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Float32 = decodeFloat4(vr)
	return vr.Err()
}

func (n NullFloat32) FormatCode() int16 { return BinaryFormatCode }

func (n NullFloat32) Encode(w *WriteBuf, oid Oid) error {
	if oid != Float4Oid {
		return SerializationError(fmt.Sprintf("NullFloat32.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeFloat32(w, oid, n.Float32)
}

// NullFloat64 represents an float8 that may be null. NullFloat64 implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullFloat64 struct {
	Float64 float64
	Valid   bool // Valid is true if Float64 is not NULL
}

func (n *NullFloat64) Scan(vr *ValueReader) error {
	if vr.Type().DataType != Float8Oid {
		return SerializationError(fmt.Sprintf("NullFloat64.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Float64, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Float64 = decodeFloat8(vr)
	return vr.Err()
}

func (n NullFloat64) FormatCode() int16 { return BinaryFormatCode }

func (n NullFloat64) Encode(w *WriteBuf, oid Oid) error {
	if oid != Float8Oid {
		return SerializationError(fmt.Sprintf("NullFloat64.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeFloat64(w, oid, n.Float64)
}

// NullString represents an string that may be null. NullString implements the
// Scanner Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullString struct {
	String string
	Valid  bool // Valid is true if String is not NULL
}

func (s *NullString) Scan(vr *ValueReader) error {
	// Not checking oid as so we can scan anything into into a NullString - may revisit this decision later

	if vr.Len() == -1 {
		s.String, s.Valid = "", false
		return nil
	}

	s.Valid = true
	s.String = decodeText(vr)
	return vr.Err()
}

func (n NullString) FormatCode() int16 { return TextFormatCode }

func (s NullString) Encode(w *WriteBuf, oid Oid) error {
	if !s.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeString(w, oid, s.String)
}

// NullInt16 represents an smallint that may be null. NullInt16 implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullInt16 struct {
	Int16 int16
	Valid bool // Valid is true if Int16 is not NULL
}

func (n *NullInt16) Scan(vr *ValueReader) error {
	if vr.Type().DataType != Int2Oid {
		return SerializationError(fmt.Sprintf("NullInt16.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Int16, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int16 = decodeInt2(vr)
	return vr.Err()
}

func (n NullInt16) FormatCode() int16 { return BinaryFormatCode }

func (n NullInt16) Encode(w *WriteBuf, oid Oid) error {
	if oid != Int2Oid {
		return SerializationError(fmt.Sprintf("NullInt16.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeInt16(w, oid, n.Int16)
}

// NullInt32 represents an integer that may be null. NullInt32 implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullInt32 struct {
	Int32 int32
	Valid bool // Valid is true if Int32 is not NULL
}

func (n *NullInt32) Scan(vr *ValueReader) error {
	if vr.Type().DataType != Int4Oid {
		return SerializationError(fmt.Sprintf("NullInt32.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Int32, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int32 = decodeInt4(vr)
	return vr.Err()
}

func (n NullInt32) FormatCode() int16 { return BinaryFormatCode }

func (n NullInt32) Encode(w *WriteBuf, oid Oid) error {
	if oid != Int4Oid {
		return SerializationError(fmt.Sprintf("NullInt32.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeInt32(w, oid, n.Int32)
}

// NullInt64 represents an bigint that may be null. NullInt64 implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullInt64 struct {
	Int64 int64
	Valid bool // Valid is true if Int64 is not NULL
}

func (n *NullInt64) Scan(vr *ValueReader) error {
	if vr.Type().DataType != Int8Oid {
		return SerializationError(fmt.Sprintf("NullInt64.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Int64, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int64 = decodeInt8(vr)
	return vr.Err()
}

func (n NullInt64) FormatCode() int16 { return BinaryFormatCode }

func (n NullInt64) Encode(w *WriteBuf, oid Oid) error {
	if oid != Int8Oid {
		return SerializationError(fmt.Sprintf("NullInt64.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeInt64(w, oid, n.Int64)
}

// NullBool represents an bool that may be null. NullBool implements the Scanner
// and Encoder interfaces so it may be used both as an argument to Query[Row]
// and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullBool struct {
	Bool  bool
	Valid bool // Valid is true if Bool is not NULL
}

func (n *NullBool) Scan(vr *ValueReader) error {
	if vr.Type().DataType != BoolOid {
		return SerializationError(fmt.Sprintf("NullBool.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Bool, n.Valid = false, false
		return nil
	}
	n.Valid = true
	n.Bool = decodeBool(vr)
	return vr.Err()
}

func (n NullBool) FormatCode() int16 { return BinaryFormatCode }

func (n NullBool) Encode(w *WriteBuf, oid Oid) error {
	if oid != BoolOid {
		return SerializationError(fmt.Sprintf("NullBool.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeBool(w, oid, n.Bool)
}

// NullTime represents an time.Time that may be null. NullTime implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan. It corresponds with the PostgreSQL
// types timestamptz, timestamp, and date.
//
// If Valid is false then the value is NULL.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

func (n *NullTime) Scan(vr *ValueReader) error {
	oid := vr.Type().DataType
	if oid != TimestampTzOid && oid != TimestampOid && oid != DateOid {
		return SerializationError(fmt.Sprintf("NullTime.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Time, n.Valid = time.Time{}, false
		return nil
	}

	n.Valid = true
	switch oid {
	case TimestampTzOid:
		n.Time = decodeTimestampTz(vr)
	case TimestampOid:
		n.Time = decodeTimestamp(vr)
	case DateOid:
		n.Time = decodeDate(vr)
	}

	return vr.Err()
}

func (n NullTime) FormatCode() int16 { return BinaryFormatCode }

func (n NullTime) Encode(w *WriteBuf, oid Oid) error {
	if oid != TimestampTzOid && oid != TimestampOid && oid != DateOid {
		return SerializationError(fmt.Sprintf("NullTime.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeTime(w, oid, n.Time)
}

// Hstore represents an hstore column. It does not support a null column or null
// key values (use NullHstore for this). Hstore implements the Scanner and
// Encoder interfaces so it may be used both as an argument to Query[Row] and a
// destination for Scan.
type Hstore map[string]string

func (h *Hstore) Scan(vr *ValueReader) error {
	//oid for hstore not standardized, so we check its type name
	if vr.Type().DataTypeName != "hstore" {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode type %s into Hstore", vr.Type().DataTypeName)))
		return nil
	}

	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null column into Hstore"))
		return nil
	}

	switch vr.Type().FormatCode {
	case TextFormatCode:
		m, err := parseHstoreToMap(vr.ReadString(vr.Len()))
		if err != nil {
			vr.Fatal(ProtocolError(fmt.Sprintf("Can't decode hstore column: %v", err)))
			return nil
		}
		hm := Hstore(m)
		*h = hm
		return nil
	case BinaryFormatCode:
		vr.Fatal(ProtocolError("Can't decode binary hstore"))
		return nil
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}
}

func (h Hstore) FormatCode() int16 { return TextFormatCode }

func (h Hstore) Encode(w *WriteBuf, oid Oid) error {
	var buf bytes.Buffer

	i := 0
	for k, v := range h {
		i++
		ks := strings.Replace(k, `\`, `\\`, -1)
		ks = strings.Replace(ks, `"`, `\"`, -1)
		vs := strings.Replace(v, `\`, `\\`, -1)
		vs = strings.Replace(vs, `"`, `\"`, -1)
		buf.WriteString(fmt.Sprintf(`"%s"=>"%s"`, ks, vs))
		if i < len(h) {
			buf.WriteString(", ")
		}
	}
	w.WriteInt32(int32(buf.Len()))
	w.WriteBytes(buf.Bytes())
	return nil
}

// NullHstore represents an hstore column that can be null or have null values
// associated with its keys.  NullHstore implements the Scanner and Encoder
// interfaces so it may be used both as an argument to Query[Row] and a
// destination for Scan.
//
// If Valid is false, then the value of the entire hstore column is NULL
// If any of the NullString values in Store has Valid set to false, the key
// appears in the hstore column, but its value is explicitly set to NULL.
type NullHstore struct {
	Hstore map[string]NullString
	Valid  bool
}

func (h *NullHstore) Scan(vr *ValueReader) error {
	//oid for hstore not standardized, so we check its type name
	if vr.Type().DataTypeName != "hstore" {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode type %s into NullHstore", vr.Type().DataTypeName)))
		return nil
	}

	if vr.Len() == -1 {
		h.Valid = false
		return nil
	}

	switch vr.Type().FormatCode {
	case TextFormatCode:
		store, err := parseHstoreToNullHstore(vr.ReadString(vr.Len()))
		if err != nil {
			vr.Fatal(ProtocolError(fmt.Sprintf("Can't decode hstore column: %v", err)))
			return nil
		}
		h.Valid = true
		h.Hstore = store
		return nil
	case BinaryFormatCode:
		vr.Fatal(ProtocolError("Can't decode binary hstore"))
		return nil
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}
}

func (h NullHstore) FormatCode() int16 { return TextFormatCode }

func (h NullHstore) Encode(w *WriteBuf, oid Oid) error {
	var buf bytes.Buffer

	if !h.Valid {
		w.WriteInt32(-1)
		return nil
	}

	i := 0
	for k, v := range h.Hstore {
		i++
		ks := strings.Replace(k, `\`, `\\`, -1)
		ks = strings.Replace(ks, `"`, `\"`, -1)
		if v.Valid {
			vs := strings.Replace(v.String, `\`, `\\`, -1)
			vs = strings.Replace(vs, `"`, `\"`, -1)
			buf.WriteString(fmt.Sprintf(`"%s"=>"%s"`, ks, vs))
		} else {
			buf.WriteString(fmt.Sprintf(`"%s"=>NULL`, ks))
		}
		if i < len(h.Hstore) {
			buf.WriteString(", ")
		}
	}
	w.WriteInt32(int32(buf.Len()))
	w.WriteBytes(buf.Bytes())
	return nil
}

// Encode encodes arg into wbuf as the type oid. This allows implementations
// of the Encoder interface to delegate the actual work of encoding to the
// built-in functionality.
func Encode(wbuf *WriteBuf, oid Oid, arg interface{}) error {
	if arg == nil {
		wbuf.WriteInt32(-1)
		return nil
	}

	switch arg := arg.(type) {
	case Encoder:
		return arg.Encode(wbuf, oid)
	case driver.Valuer:
		v, err := arg.Value()
		if err != nil {
			return err
		}
		return Encode(wbuf, oid, v)
	case string:
		return encodeString(wbuf, oid, arg)
	case []byte:
		return encodeByteSlice(wbuf, oid, arg)
	case [][]byte:
		return encodeByteSliceSlice(wbuf, oid, arg)
	}

	refVal := reflect.ValueOf(arg)

	if refVal.Kind() == reflect.Ptr {
		if refVal.IsNil() {
			wbuf.WriteInt32(-1)
			return nil
		} else {
			arg = refVal.Elem().Interface()
			return Encode(wbuf, oid, arg)
		}
	}

	if oid == JsonOid || oid == JsonbOid {
		return encodeJSON(wbuf, oid, arg)
	}

	switch arg := arg.(type) {
	case []string:
		return encodeStringSlice(wbuf, oid, arg)
	case bool:
		return encodeBool(wbuf, oid, arg)
	case []bool:
		return encodeBoolSlice(wbuf, oid, arg)
	case int:
		return encodeInt(wbuf, oid, arg)
	case uint:
		return encodeUInt(wbuf, oid, arg)
	case int8:
		return encodeInt8(wbuf, oid, arg)
	case uint8:
		return encodeUInt8(wbuf, oid, arg)
	case int16:
		return encodeInt16(wbuf, oid, arg)
	case []int16:
		return encodeInt16Slice(wbuf, oid, arg)
	case uint16:
		return encodeUInt16(wbuf, oid, arg)
	case []uint16:
		return encodeUInt16Slice(wbuf, oid, arg)
	case int32:
		return encodeInt32(wbuf, oid, arg)
	case []int32:
		return encodeInt32Slice(wbuf, oid, arg)
	case uint32:
		return encodeUInt32(wbuf, oid, arg)
	case []uint32:
		return encodeUInt32Slice(wbuf, oid, arg)
	case int64:
		return encodeInt64(wbuf, oid, arg)
	case []int64:
		return encodeInt64Slice(wbuf, oid, arg)
	case uint64:
		return encodeUInt64(wbuf, oid, arg)
	case []uint64:
		return encodeUInt64Slice(wbuf, oid, arg)
	case float32:
		return encodeFloat32(wbuf, oid, arg)
	case []float32:
		return encodeFloat32Slice(wbuf, oid, arg)
	case float64:
		return encodeFloat64(wbuf, oid, arg)
	case []float64:
		return encodeFloat64Slice(wbuf, oid, arg)
	case time.Time:
		return encodeTime(wbuf, oid, arg)
	case []time.Time:
		return encodeTimeSlice(wbuf, oid, arg)
	case net.IP:
		return encodeIP(wbuf, oid, arg)
	case []net.IP:
		return encodeIPSlice(wbuf, oid, arg)
	case net.IPNet:
		return encodeIPNet(wbuf, oid, arg)
	case []net.IPNet:
		return encodeIPNetSlice(wbuf, oid, arg)
	case Oid:
		return encodeOid(wbuf, oid, arg)
	default:
		if strippedArg, ok := stripNamedType(&refVal); ok {
			return Encode(wbuf, oid, strippedArg)
		}
		return SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
	}
}

func stripNamedType(val *reflect.Value) (interface{}, bool) {
	switch val.Kind() {
	case reflect.Int:
		return int(val.Int()), true
	case reflect.Int8:
		return int8(val.Int()), true
	case reflect.Int16:
		return int16(val.Int()), true
	case reflect.Int32:
		return int32(val.Int()), true
	case reflect.Int64:
		return int64(val.Int()), true
	case reflect.Uint:
		return uint(val.Uint()), true
	case reflect.Uint8:
		return uint8(val.Uint()), true
	case reflect.Uint16:
		return uint16(val.Uint()), true
	case reflect.Uint32:
		return uint32(val.Uint()), true
	case reflect.Uint64:
		return uint64(val.Uint()), true
	case reflect.String:
		return val.String(), true
	}

	return nil, false
}

// Decode decodes from vr into d. d must be a pointer. This allows
// implementations of the Decoder interface to delegate the actual work of
// decoding to the built-in functionality.
func Decode(vr *ValueReader, d interface{}) error {
	switch v := d.(type) {
	case *bool:
		*v = decodeBool(vr)
	case *int:
		n := decodeInt(vr)
		if n < int64(minInt) {
			return fmt.Errorf("%d is less than minimum value for int", n)
		} else if n > int64(maxInt) {
			return fmt.Errorf("%d is greater than maximum value for int", n)
		}
		*v = int(n)
	case *int8:
		n := decodeInt(vr)
		if n < math.MinInt8 {
			return fmt.Errorf("%d is less than minimum value for int8", n)
		} else if n > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for int8", n)
		}
		*v = int8(n)
	case *int16:
		n := decodeInt(vr)
		if n < math.MinInt16 {
			return fmt.Errorf("%d is less than minimum value for int16", n)
		} else if n > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for int16", n)
		}
		*v = int16(n)
	case *int32:
		n := decodeInt(vr)
		if n < math.MinInt32 {
			return fmt.Errorf("%d is less than minimum value for int32", n)
		} else if n > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for int32", n)
		}
		*v = int32(n)
	case *int64:
		n := decodeInt(vr)
		if n < math.MinInt64 {
			return fmt.Errorf("%d is less than minimum value for int64", n)
		} else if n > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for int64", n)
		}
		*v = int64(n)
	case *uint:
		n := decodeInt(vr)
		if n < 0 {
			return fmt.Errorf("%d is less than zero for uint8", n)
		} else if maxInt == math.MaxInt32 && n > math.MaxUint32 {
			return fmt.Errorf("%d is greater than maximum value for uint", n)
		}
		*v = uint(n)
	case *uint8:
		n := decodeInt(vr)
		if n < 0 {
			return fmt.Errorf("%d is less than zero for uint8", n)
		} else if n > math.MaxUint8 {
			return fmt.Errorf("%d is greater than maximum value for uint8", n)
		}
		*v = uint8(n)
	case *uint16:
		n := decodeInt(vr)
		if n < 0 {
			return fmt.Errorf("%d is less than zero for uint16", n)
		} else if n > math.MaxUint16 {
			return fmt.Errorf("%d is greater than maximum value for uint16", n)
		}
		*v = uint16(n)
	case *uint32:
		n := decodeInt(vr)
		if n < 0 {
			return fmt.Errorf("%d is less than zero for uint32", n)
		} else if n > math.MaxUint32 {
			return fmt.Errorf("%d is greater than maximum value for uint32", n)
		}
		*v = uint32(n)
	case *uint64:
		n := decodeInt(vr)
		if n < 0 {
			return fmt.Errorf("%d is less than zero for uint64", n)
		}
		*v = uint64(n)
	case *Oid:
		*v = decodeOid(vr)
	case *string:
		*v = decodeText(vr)
	case *float32:
		*v = decodeFloat4(vr)
	case *float64:
		*v = decodeFloat8(vr)
	case *[]bool:
		*v = decodeBoolArray(vr)
	case *[]int16:
		*v = decodeInt2Array(vr)
	case *[]uint16:
		*v = decodeInt2ArrayToUInt(vr)
	case *[]int32:
		*v = decodeInt4Array(vr)
	case *[]uint32:
		*v = decodeInt4ArrayToUInt(vr)
	case *[]int64:
		*v = decodeInt8Array(vr)
	case *[]uint64:
		*v = decodeInt8ArrayToUInt(vr)
	case *[]float32:
		*v = decodeFloat4Array(vr)
	case *[]float64:
		*v = decodeFloat8Array(vr)
	case *[]string:
		*v = decodeTextArray(vr)
	case *[]time.Time:
		*v = decodeTimestampArray(vr)
	case *[][]byte:
		*v = decodeByteaArray(vr)
	case *[]interface{}:
		*v = decodeRecord(vr)
	case *time.Time:
		switch vr.Type().DataType {
		case DateOid:
			*v = decodeDate(vr)
		case TimestampTzOid:
			*v = decodeTimestampTz(vr)
		case TimestampOid:
			*v = decodeTimestamp(vr)
		default:
			return fmt.Errorf("Can't convert OID %v to time.Time", vr.Type().DataType)
		}
	case *net.IP:
		ipnet := decodeInet(vr)
		if oneCount, bitCount := ipnet.Mask.Size(); oneCount != bitCount {
			return fmt.Errorf("Cannot decode netmask into *net.IP")
		}
		*v = ipnet.IP
	case *[]net.IP:
		ipnets := decodeInetArray(vr)
		ips := make([]net.IP, len(ipnets))
		for i, ipnet := range ipnets {
			if oneCount, bitCount := ipnet.Mask.Size(); oneCount != bitCount {
				return fmt.Errorf("Cannot decode netmask into *net.IP")
			}
			ips[i] = ipnet.IP
		}
		*v = ips
	case *net.IPNet:
		*v = decodeInet(vr)
	case *[]net.IPNet:
		*v = decodeInetArray(vr)
	default:
		if v := reflect.ValueOf(d); v.Kind() == reflect.Ptr {
			el := v.Elem()
			switch el.Kind() {
			// if d is a pointer to pointer, strip the pointer and try again
			case reflect.Ptr:
				// -1 is a null value
				if vr.Len() == -1 {
					if !el.IsNil() {
						// if the destination pointer is not nil, nil it out
						el.Set(reflect.Zero(el.Type()))
					}
					return nil
				} else {
					if el.IsNil() {
						// allocate destination
						el.Set(reflect.New(el.Type().Elem()))
					}
					d = el.Interface()
					return Decode(vr, d)
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				n := decodeInt(vr)
				if el.OverflowInt(n) {
					return fmt.Errorf("Scan cannot decode %d into %T", n, d)
				}
				el.SetInt(n)
				return nil
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				n := decodeInt(vr)
				if n < 0 {
					return fmt.Errorf("%d is less than zero for %T", n, d)
				}
				if el.OverflowUint(uint64(n)) {
					return fmt.Errorf("Scan cannot decode %d into %T", n, d)
				}
				el.SetUint(uint64(n))
				return nil
			case reflect.String:
				el.SetString(decodeText(vr))
				return nil
			}
		}
		return fmt.Errorf("Scan cannot decode into %T", d)
	}

	return nil
}

func decodeBool(vr *ValueReader) bool {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into bool"))
		return false
	}

	if vr.Type().DataType != BoolOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into bool", vr.Type().DataType)))
		return false
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return false
	}

	if vr.Len() != 1 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an bool: %d", vr.Len())))
		return false
	}

	b := vr.ReadByte()
	return b != 0
}

func encodeBool(w *WriteBuf, oid Oid, value bool) error {
	if oid != BoolOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "bool", oid)
	}

	w.WriteInt32(1)

	var n byte
	if value {
		n = 1
	}

	w.WriteByte(n)

	return nil
}

func decodeInt(vr *ValueReader) int64 {
	switch vr.Type().DataType {
	case Int2Oid:
		return int64(decodeInt2(vr))
	case Int4Oid:
		return int64(decodeInt4(vr))
	case Int8Oid:
		return int64(decodeInt8(vr))
	}

	vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into any integer type", vr.Type().DataType)))
	return 0
}

func decodeInt8(vr *ValueReader) int64 {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into int64"))
		return 0
	}

	if vr.Type().DataType != Int8Oid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into int8", vr.Type().DataType)))
		return 0
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if vr.Len() != 8 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int8: %d", vr.Len())))
		return 0
	}

	return vr.ReadInt64()
}

func decodeInt2(vr *ValueReader) int16 {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into int16"))
		return 0
	}

	if vr.Type().DataType != Int2Oid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into int16", vr.Type().DataType)))
		return 0
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if vr.Len() != 2 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int2: %d", vr.Len())))
		return 0
	}

	return vr.ReadInt16()
}

func encodeInt(w *WriteBuf, oid Oid, value int) error {
	switch oid {
	case Int2Oid:
		if value < math.MinInt16 {
			return fmt.Errorf("%d is less than min pg:int2", value)
		} else if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than max pg:int2", value)
		}
		w.WriteInt32(2)
		w.WriteInt16(int16(value))
	case Int4Oid:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is less than min pg:int4", value)
		} else if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than max pg:int4", value)
		}
		w.WriteInt32(4)
		w.WriteInt32(int32(value))
	case Int8Oid:
		if int64(value) <= int64(math.MaxInt64) {
			w.WriteInt32(8)
			w.WriteInt64(int64(value))
		} else {
			return fmt.Errorf("%d is larger than max int64 %d", value, int64(math.MaxInt64))
		}
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "int8", oid)
	}

	return nil
}

func encodeUInt(w *WriteBuf, oid Oid, value uint) error {
	switch oid {
	case Int2Oid:
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than max pg:int2", value)
		}
		w.WriteInt32(2)
		w.WriteInt16(int16(value))
	case Int4Oid:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than max pg:int4", value)
		}
		w.WriteInt32(4)
		w.WriteInt32(int32(value))
	case Int8Oid:
		//****** Changed value to int64(value) and math.MaxInt64 to int64(math.MaxInt64)
		if int64(value) > int64(math.MaxInt64) {
			return fmt.Errorf("%d is greater than max pg:int8", value)
		}
		w.WriteInt32(8)
		w.WriteInt64(int64(value))

	default:
		return fmt.Errorf("cannot encode %s into oid %v", "uint8", oid)
	}

	return nil
}

func encodeInt8(w *WriteBuf, oid Oid, value int8) error {
	switch oid {
	case Int2Oid:
		w.WriteInt32(2)
		w.WriteInt16(int16(value))
	case Int4Oid:
		w.WriteInt32(4)
		w.WriteInt32(int32(value))
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(value))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "int8", oid)
	}

	return nil
}

func encodeUInt8(w *WriteBuf, oid Oid, value uint8) error {
	switch oid {
	case Int2Oid:
		w.WriteInt32(2)
		w.WriteInt16(int16(value))
	case Int4Oid:
		w.WriteInt32(4)
		w.WriteInt32(int32(value))
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(value))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "uint8", oid)
	}

	return nil
}

func encodeInt16(w *WriteBuf, oid Oid, value int16) error {
	switch oid {
	case Int2Oid:
		w.WriteInt32(2)
		w.WriteInt16(value)
	case Int4Oid:
		w.WriteInt32(4)
		w.WriteInt32(int32(value))
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(value))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "int16", oid)
	}

	return nil
}

func encodeUInt16(w *WriteBuf, oid Oid, value uint16) error {
	switch oid {
	case Int2Oid:
		if value <= math.MaxInt16 {
			w.WriteInt32(2)
			w.WriteInt16(int16(value))
		} else {
			return fmt.Errorf("%d is greater than max int16 %d", value, math.MaxInt16)
		}
	case Int4Oid:
		w.WriteInt32(4)
		w.WriteInt32(int32(value))
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(value))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "int16", oid)
	}

	return nil
}

func encodeInt32(w *WriteBuf, oid Oid, value int32) error {
	switch oid {
	case Int2Oid:
		if value <= math.MaxInt16 {
			w.WriteInt32(2)
			w.WriteInt16(int16(value))
		} else {
			return fmt.Errorf("%d is greater than max int16 %d", value, math.MaxInt16)
		}
	case Int4Oid:
		w.WriteInt32(4)
		w.WriteInt32(value)
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(value))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "int32", oid)
	}

	return nil
}

func encodeUInt32(w *WriteBuf, oid Oid, value uint32) error {
	switch oid {
	case Int2Oid:
		if value <= math.MaxInt16 {
			w.WriteInt32(2)
			w.WriteInt16(int16(value))
		} else {
			return fmt.Errorf("%d is greater than max int16 %d", value, math.MaxInt16)
		}
	case Int4Oid:
		if value <= math.MaxInt32 {
			w.WriteInt32(4)
			w.WriteInt32(int32(value))
		} else {
			return fmt.Errorf("%d is greater than max int32 %d", value, math.MaxInt32)
		}
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(value))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "uint32", oid)
	}

	return nil
}

func encodeInt64(w *WriteBuf, oid Oid, value int64) error {
	switch oid {
	case Int2Oid:
		if value <= math.MaxInt16 {
			w.WriteInt32(2)
			w.WriteInt16(int16(value))
		} else {
			return fmt.Errorf("%d is greater than max int16 %d", value, math.MaxInt16)
		}
	case Int4Oid:
		if value <= math.MaxInt32 {
			w.WriteInt32(4)
			w.WriteInt32(int32(value))
		} else {
			return fmt.Errorf("%d is greater than max int32 %d", value, math.MaxInt32)
		}
	case Int8Oid:
		w.WriteInt32(8)
		w.WriteInt64(value)
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "int64", oid)
	}

	return nil
}

func encodeUInt64(w *WriteBuf, oid Oid, value uint64) error {
	switch oid {
	case Int2Oid:
		if value <= math.MaxInt16 {
			w.WriteInt32(2)
			w.WriteInt16(int16(value))
		} else {
			return fmt.Errorf("%d is greater than max int16 %d", value, math.MaxInt16)
		}
	case Int4Oid:
		if value <= math.MaxInt32 {
			w.WriteInt32(4)
			w.WriteInt32(int32(value))
		} else {
			return fmt.Errorf("%d is greater than max int32 %d", value, math.MaxInt32)
		}
	case Int8Oid:

		if value <= math.MaxInt64 {
			w.WriteInt32(8)
			w.WriteInt64(int64(value))
		} else {
			return fmt.Errorf("%d is greater than max int64 %d", value, int64(math.MaxInt64))
		}
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "uint64", oid)
	}

	return nil
}

func decodeInt4(vr *ValueReader) int32 {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into int32"))
		return 0
	}

	if vr.Type().DataType != Int4Oid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into int32", vr.Type().DataType)))
		return 0
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if vr.Len() != 4 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int4: %d", vr.Len())))
		return 0
	}

	return vr.ReadInt32()
}

func decodeOid(vr *ValueReader) Oid {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into Oid"))
		return 0
	}

	if vr.Type().DataType != OidOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into pgx.Oid", vr.Type().DataType)))
		return 0
	}

	// Oid needs to decode text format because it is used in loadPgTypes
	switch vr.Type().FormatCode {
	case TextFormatCode:
		s := vr.ReadString(vr.Len())
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			vr.Fatal(ProtocolError(fmt.Sprintf("Received invalid Oid: %v", s)))
		}
		return Oid(n)
	case BinaryFormatCode:
		if vr.Len() != 4 {
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an Oid: %d", vr.Len())))
			return 0
		}
		return Oid(vr.ReadInt32())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return Oid(0)
	}
}

func encodeOid(w *WriteBuf, oid Oid, value Oid) error {
	if oid != OidOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "pgx.Oid", oid)
	}

	w.WriteInt32(4)
	w.WriteInt32(int32(value))

	return nil
}

func decodeFloat4(vr *ValueReader) float32 {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into float32"))
		return 0
	}

	if vr.Type().DataType != Float4Oid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into float32", vr.Type().DataType)))
		return 0
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if vr.Len() != 4 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float4: %d", vr.Len())))
		return 0
	}

	i := vr.ReadInt32()
	return math.Float32frombits(uint32(i))
}

func encodeFloat32(w *WriteBuf, oid Oid, value float32) error {
	switch oid {
	case Float4Oid:
		w.WriteInt32(4)
		w.WriteInt32(int32(math.Float32bits(value)))
	case Float8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(math.Float64bits(float64(value))))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "float32", oid)
	}

	return nil
}

func decodeFloat8(vr *ValueReader) float64 {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into float64"))
		return 0
	}

	if vr.Type().DataType != Float8Oid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into float64", vr.Type().DataType)))
		return 0
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if vr.Len() != 8 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float8: %d", vr.Len())))
		return 0
	}

	i := vr.ReadInt64()
	return math.Float64frombits(uint64(i))
}

func encodeFloat64(w *WriteBuf, oid Oid, value float64) error {
	switch oid {
	case Float8Oid:
		w.WriteInt32(8)
		w.WriteInt64(int64(math.Float64bits(value)))
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "float64", oid)
	}

	return nil
}

func decodeText(vr *ValueReader) string {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into string"))
		return ""
	}

	return vr.ReadString(vr.Len())
}

func encodeString(w *WriteBuf, oid Oid, value string) error {
	w.WriteInt32(int32(len(value)))
	w.WriteBytes([]byte(value))
	return nil
}

func decodeBytea(vr *ValueReader) []byte {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != ByteaOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []byte", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	return vr.ReadBytes(vr.Len())
}

func encodeByteSlice(w *WriteBuf, oid Oid, value []byte) error {
	w.WriteInt32(int32(len(value)))
	w.WriteBytes(value)

	return nil
}

func decodeJSON(vr *ValueReader, d interface{}) error {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != JsonOid && vr.Type().DataType != JsonbOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into json", vr.Type().DataType)))
	}

	bytes := vr.ReadBytes(vr.Len())
	err := json.Unmarshal(bytes, d)
	if err != nil {
		vr.Fatal(err)
	}
	return err
}

func encodeJSON(w *WriteBuf, oid Oid, value interface{}) error {
	if oid != JsonOid && oid != JsonbOid {
		return fmt.Errorf("cannot encode JSON into oid %v", oid)
	}

	s, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Failed to encode json from type: %T", value)
	}

	w.WriteInt32(int32(len(s)))
	w.WriteBytes(s)

	return nil
}

func decodeDate(vr *ValueReader) time.Time {
	var zeroTime time.Time

	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into time.Time"))
		return zeroTime
	}

	if vr.Type().DataType != DateOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into time.Time", vr.Type().DataType)))
		return zeroTime
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return zeroTime
	}

	if vr.Len() != 4 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an date: %d", vr.Len())))
	}
	dayOffset := vr.ReadInt32()
	return time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.Local)
}

func encodeTime(w *WriteBuf, oid Oid, value time.Time) error {
	switch oid {
	case DateOid:
		tUnix := time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC).Unix()
		dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

		secSinceDateEpoch := tUnix - dateEpoch
		daysSinceDateEpoch := secSinceDateEpoch / 86400

		w.WriteInt32(4)
		w.WriteInt32(int32(daysSinceDateEpoch))

		return nil
	case TimestampTzOid, TimestampOid:
		microsecSinceUnixEpoch := value.Unix()*1000000 + int64(value.Nanosecond())/1000
		microsecSinceY2K := microsecSinceUnixEpoch - microsecFromUnixEpochToY2K

		w.WriteInt32(8)
		w.WriteInt64(microsecSinceY2K)

		return nil
	default:
		return fmt.Errorf("cannot encode %s into oid %v", "time.Time", oid)
	}
}

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func decodeTimestampTz(vr *ValueReader) time.Time {
	var zeroTime time.Time

	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into time.Time"))
		return zeroTime
	}

	if vr.Type().DataType != TimestampTzOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into time.Time", vr.Type().DataType)))
		return zeroTime
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return zeroTime
	}

	if vr.Len() != 8 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an timestamptz: %d", vr.Len())))
		return zeroTime
	}

	microsecSinceY2K := vr.ReadInt64()
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
}

func decodeTimestamp(vr *ValueReader) time.Time {
	var zeroTime time.Time

	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into timestamp"))
		return zeroTime
	}

	if vr.Type().DataType != TimestampOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into time.Time", vr.Type().DataType)))
		return zeroTime
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return zeroTime
	}

	if vr.Len() != 8 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an timestamp: %d", vr.Len())))
		return zeroTime
	}

	microsecSinceY2K := vr.ReadInt64()
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
}

func decodeInet(vr *ValueReader) net.IPNet {
	var zero net.IPNet

	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into net.IPNet"))
		return zero
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return zero
	}

	pgType := vr.Type()
	if pgType.DataType != InetOid && pgType.DataType != CidrOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into %s", pgType.DataType, pgType.Name)))
		return zero
	}
	if vr.Len() != 8 && vr.Len() != 20 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for a %s: %d", pgType.Name, vr.Len())))
		return zero
	}

	vr.ReadByte() // ignore family
	bits := vr.ReadByte()
	vr.ReadByte() // ignore is_cidr
	addressLength := vr.ReadByte()

	var ipnet net.IPNet
	ipnet.IP = vr.ReadBytes(int32(addressLength))
	ipnet.Mask = net.CIDRMask(int(bits), int(addressLength)*8)

	return ipnet
}

func encodeIPNet(w *WriteBuf, oid Oid, value net.IPNet) error {
	if oid != InetOid && oid != CidrOid {
		return fmt.Errorf("cannot encode %s into oid %v", "net.IPNet", oid)
	}

	var size int32
	var family byte
	switch len(value.IP) {
	case net.IPv4len:
		size = 8
		family = *w.conn.pgsql_af_inet
	case net.IPv6len:
		size = 20
		family = *w.conn.pgsql_af_inet6
	default:
		return fmt.Errorf("Unexpected IP length: %v", len(value.IP))
	}

	w.WriteInt32(size)
	w.WriteByte(family)
	ones, _ := value.Mask.Size()
	w.WriteByte(byte(ones))
	w.WriteByte(0) // is_cidr is ignored on server
	w.WriteByte(byte(len(value.IP)))
	w.WriteBytes(value.IP)

	return nil
}

func encodeIP(w *WriteBuf, oid Oid, value net.IP) error {
	if oid != InetOid && oid != CidrOid {
		return fmt.Errorf("cannot encode %s into oid %v", "net.IP", oid)
	}

	var ipnet net.IPNet
	ipnet.IP = value
	bitCount := len(value) * 8
	ipnet.Mask = net.CIDRMask(bitCount, bitCount)
	return encodeIPNet(w, oid, ipnet)
}

func decodeRecord(vr *ValueReader) []interface{} {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	if vr.Type().DataType != RecordOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []interface{}", vr.Type().DataType)))
		return nil
	}

	valueCount := vr.ReadInt32()
	record := make([]interface{}, 0, int(valueCount))

	for i := int32(0); i < valueCount; i++ {
		fd := FieldDescription{FormatCode: BinaryFormatCode}
		fieldVR := ValueReader{mr: vr.mr, fd: &fd}
		fd.DataType = vr.ReadOid()
		fieldVR.valueBytesRemaining = vr.ReadInt32()
		vr.valueBytesRemaining -= fieldVR.valueBytesRemaining

		switch fd.DataType {
		case BoolOid:
			record = append(record, decodeBool(&fieldVR))
		case ByteaOid:
			record = append(record, decodeBytea(&fieldVR))
		case Int8Oid:
			record = append(record, decodeInt8(&fieldVR))
		case Int2Oid:
			record = append(record, decodeInt2(&fieldVR))
		case Int4Oid:
			record = append(record, decodeInt4(&fieldVR))
		case OidOid:
			record = append(record, decodeOid(&fieldVR))
		case Float4Oid:
			record = append(record, decodeFloat4(&fieldVR))
		case Float8Oid:
			record = append(record, decodeFloat8(&fieldVR))
		case DateOid:
			record = append(record, decodeDate(&fieldVR))
		case TimestampTzOid:
			record = append(record, decodeTimestampTz(&fieldVR))
		case TimestampOid:
			record = append(record, decodeTimestamp(&fieldVR))
		case InetOid, CidrOid:
			record = append(record, decodeInet(&fieldVR))
		case TextOid, VarcharOid, UnknownOid:
			record = append(record, decodeText(&fieldVR))
		default:
			vr.Fatal(fmt.Errorf("decodeRecord cannot decode oid %d", fd.DataType))
			return nil
		}

		// Consume any remaining data
		if fieldVR.Len() > 0 {
			fieldVR.ReadBytes(fieldVR.Len())
		}

		if fieldVR.Err() != nil {
			vr.Fatal(fieldVR.Err())
			return nil
		}
	}

	return record
}

func decode1dArrayHeader(vr *ValueReader) (length int32, err error) {
	numDims := vr.ReadInt32()
	if numDims > 1 {
		return 0, ProtocolError(fmt.Sprintf("Expected array to have 0 or 1 dimension, but it had %v", numDims))
	}

	vr.ReadInt32() // 0 if no nulls / 1 if there is one or more nulls -- but we don't care
	vr.ReadInt32() // element oid

	if numDims == 0 {
		return 0, nil
	}

	length = vr.ReadInt32()

	idxFirstElem := vr.ReadInt32()
	if idxFirstElem != 1 {
		return 0, ProtocolError(fmt.Sprintf("Expected array's first element to start a index 1, but it is %d", idxFirstElem))
	}

	return length, nil
}

func decodeBoolArray(vr *ValueReader) []bool {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != BoolArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []bool", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]bool, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 1:
			if vr.ReadByte() == 1 {
				a[i] = true
			}
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an bool element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeBoolSlice(w *WriteBuf, oid Oid, slice []bool) error {
	if oid != BoolArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]bool", oid)
	}

	encodeArrayHeader(w, BoolOid, len(slice), 5)
	for _, v := range slice {
		w.WriteInt32(1)
		var b byte
		if v {
			b = 1
		}
		w.WriteByte(b)
	}

	return nil
}

func decodeByteaArray(vr *ValueReader) [][]byte {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != ByteaArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into [][]byte", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([][]byte, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			a[i] = vr.ReadBytes(elSize)
		}
	}

	return a
}

func encodeByteSliceSlice(w *WriteBuf, oid Oid, value [][]byte) error {
	if oid != ByteaArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[][]byte", oid)
	}

	size := 20 // array header size
	for _, el := range value {
		size += 4 + len(el)
	}

	w.WriteInt32(int32(size))

	w.WriteInt32(1)                 // number of dimensions
	w.WriteInt32(0)                 // no nulls
	w.WriteInt32(int32(ByteaOid))   // type of elements
	w.WriteInt32(int32(len(value))) // number of elements
	w.WriteInt32(1)                 // index of first element

	for _, el := range value {
		encodeByteSlice(w, ByteaOid, el)
	}

	return nil
}

func decodeInt2Array(vr *ValueReader) []int16 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Int2ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []int16", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]int16, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 2:
			a[i] = vr.ReadInt16()
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int2 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func decodeInt2ArrayToUInt(vr *ValueReader) []uint16 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Int2ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []uint16", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]uint16, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 2:
			tmp := vr.ReadInt16()
			if tmp < 0 {
				vr.Fatal(ProtocolError(fmt.Sprintf("%d is less than zero for uint16", tmp)))
				return nil
			}
			a[i] = uint16(tmp)
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int2 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeInt16Slice(w *WriteBuf, oid Oid, slice []int16) error {
	if oid != Int2ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]int16", oid)
	}

	encodeArrayHeader(w, Int2Oid, len(slice), 6)
	for _, v := range slice {
		w.WriteInt32(2)
		w.WriteInt16(v)
	}

	return nil
}

func encodeUInt16Slice(w *WriteBuf, oid Oid, slice []uint16) error {
	if oid != Int2ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]uint16", oid)
	}

	encodeArrayHeader(w, Int2Oid, len(slice), 6)
	for _, v := range slice {
		if v <= math.MaxInt16 {
			w.WriteInt32(2)
			w.WriteInt16(int16(v))
		} else {
			return fmt.Errorf("%d is greater than max smallint %d", v, math.MaxInt16)
		}
	}

	return nil
}

func decodeInt4Array(vr *ValueReader) []int32 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Int4ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []int32", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]int32, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 4:
			a[i] = vr.ReadInt32()
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int4 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func decodeInt4ArrayToUInt(vr *ValueReader) []uint32 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Int4ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []uint32", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]uint32, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 4:
			tmp := vr.ReadInt32()
			if tmp < 0 {
				vr.Fatal(ProtocolError(fmt.Sprintf("%d is less than zero for uint32", tmp)))
				return nil
			}
			a[i] = uint32(tmp)
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int4 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeInt32Slice(w *WriteBuf, oid Oid, slice []int32) error {
	if oid != Int4ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]int32", oid)
	}

	encodeArrayHeader(w, Int4Oid, len(slice), 8)
	for _, v := range slice {
		w.WriteInt32(4)
		w.WriteInt32(v)
	}

	return nil
}

func encodeUInt32Slice(w *WriteBuf, oid Oid, slice []uint32) error {
	if oid != Int4ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]uint32", oid)
	}

	encodeArrayHeader(w, Int4Oid, len(slice), 8)
	for _, v := range slice {
		if v <= math.MaxInt32 {
			w.WriteInt32(4)
			w.WriteInt32(int32(v))
		} else {
			return fmt.Errorf("%d is greater than max integer %d", v, math.MaxInt32)
		}
	}

	return nil
}

func decodeInt8Array(vr *ValueReader) []int64 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Int8ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []int64", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]int64, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			a[i] = vr.ReadInt64()
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int8 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func decodeInt8ArrayToUInt(vr *ValueReader) []uint64 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Int8ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []uint64", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]uint64, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			tmp := vr.ReadInt64()
			if tmp < 0 {
				vr.Fatal(ProtocolError(fmt.Sprintf("%d is less than zero for uint64", tmp)))
				return nil
			}
			a[i] = uint64(tmp)
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int8 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeInt64Slice(w *WriteBuf, oid Oid, slice []int64) error {
	if oid != Int8ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]int64", oid)
	}

	encodeArrayHeader(w, Int8Oid, len(slice), 12)
	for _, v := range slice {
		w.WriteInt32(8)
		w.WriteInt64(v)
	}

	return nil
}

func encodeUInt64Slice(w *WriteBuf, oid Oid, slice []uint64) error {
	if oid != Int8ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]uint64", oid)
	}

	encodeArrayHeader(w, Int8Oid, len(slice), 12)
	for _, v := range slice {
		if v <= math.MaxInt64 {
			w.WriteInt32(8)
			w.WriteInt64(int64(v))
		} else {
			return fmt.Errorf("%d is greater than max bigint %d", v, int64(math.MaxInt64))
		}
	}

	return nil
}

func decodeFloat4Array(vr *ValueReader) []float32 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Float4ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []float32", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]float32, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 4:
			n := vr.ReadInt32()
			a[i] = math.Float32frombits(uint32(n))
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float4 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeFloat32Slice(w *WriteBuf, oid Oid, slice []float32) error {
	if oid != Float4ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]float32", oid)
	}

	encodeArrayHeader(w, Float4Oid, len(slice), 8)
	for _, v := range slice {
		w.WriteInt32(4)
		w.WriteInt32(int32(math.Float32bits(v)))
	}

	return nil
}

func decodeFloat8Array(vr *ValueReader) []float64 {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != Float8ArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []float64", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]float64, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			n := vr.ReadInt64()
			a[i] = math.Float64frombits(uint64(n))
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float4 element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeFloat64Slice(w *WriteBuf, oid Oid, slice []float64) error {
	if oid != Float8ArrayOid {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]float64", oid)
	}

	encodeArrayHeader(w, Float8Oid, len(slice), 12)
	for _, v := range slice {
		w.WriteInt32(8)
		w.WriteInt64(int64(math.Float64bits(v)))
	}

	return nil
}

func decodeTextArray(vr *ValueReader) []string {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != TextArrayOid && vr.Type().DataType != VarcharArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []string", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]string, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		if elSize == -1 {
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		}

		a[i] = vr.ReadString(elSize)
	}

	return a
}

func encodeStringSlice(w *WriteBuf, oid Oid, slice []string) error {
	var elOid Oid
	switch oid {
	case VarcharArrayOid:
		elOid = VarcharOid
	case TextArrayOid:
		elOid = TextOid
	default:
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]string", oid)
	}

	var totalStringSize int
	for _, v := range slice {
		totalStringSize += len(v)
	}

	size := 20 + len(slice)*4 + totalStringSize
	w.WriteInt32(int32(size))

	w.WriteInt32(1)                 // number of dimensions
	w.WriteInt32(0)                 // no nulls
	w.WriteInt32(int32(elOid))      // type of elements
	w.WriteInt32(int32(len(slice))) // number of elements
	w.WriteInt32(1)                 // index of first element

	for _, v := range slice {
		w.WriteInt32(int32(len(v)))
		w.WriteBytes([]byte(v))
	}

	return nil
}

func decodeTimestampArray(vr *ValueReader) []time.Time {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != TimestampArrayOid && vr.Type().DataType != TimestampTzArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []time.Time", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]time.Time, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			microsecSinceY2K := vr.ReadInt64()
			microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
			a[i] = time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
		case -1:
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		default:
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an time.Time element: %d", elSize)))
			return nil
		}
	}

	return a
}

func encodeTimeSlice(w *WriteBuf, oid Oid, slice []time.Time) error {
	var elOid Oid
	switch oid {
	case TimestampArrayOid:
		elOid = TimestampOid
	case TimestampTzArrayOid:
		elOid = TimestampTzOid
	default:
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]time.Time", oid)
	}

	encodeArrayHeader(w, int(elOid), len(slice), 12)
	for _, t := range slice {
		w.WriteInt32(8)
		microsecSinceUnixEpoch := t.Unix()*1000000 + int64(t.Nanosecond())/1000
		microsecSinceY2K := microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
		w.WriteInt64(microsecSinceY2K)
	}

	return nil
}

func decodeInetArray(vr *ValueReader) []net.IPNet {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != InetArrayOid && vr.Type().DataType != CidrArrayOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []net.IP", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	numElems, err := decode1dArrayHeader(vr)
	if err != nil {
		vr.Fatal(err)
		return nil
	}

	a := make([]net.IPNet, int(numElems))
	for i := 0; i < len(a); i++ {
		elSize := vr.ReadInt32()
		if elSize == -1 {
			vr.Fatal(ProtocolError("Cannot decode null element"))
			return nil
		}

		vr.ReadByte() // ignore family
		bits := vr.ReadByte()
		vr.ReadByte() // ignore is_cidr
		addressLength := vr.ReadByte()

		var ipnet net.IPNet
		ipnet.IP = vr.ReadBytes(int32(addressLength))
		ipnet.Mask = net.CIDRMask(int(bits), int(addressLength)*8)

		a[i] = ipnet
	}

	return a
}

func encodeIPNetSlice(w *WriteBuf, oid Oid, slice []net.IPNet) error {
	var elOid Oid
	switch oid {
	case InetArrayOid:
		elOid = InetOid
	case CidrArrayOid:
		elOid = CidrOid
	default:
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]net.IPNet", oid)
	}

	size := int32(20) // array header size
	for _, ipnet := range slice {
		size += 4 + 4 + int32(len(ipnet.IP)) // size of element + inet/cidr metadata + IP bytes
	}
	w.WriteInt32(int32(size))

	w.WriteInt32(1)                 // number of dimensions
	w.WriteInt32(0)                 // no nulls
	w.WriteInt32(int32(elOid))      // type of elements
	w.WriteInt32(int32(len(slice))) // number of elements
	w.WriteInt32(1)                 // index of first element

	for _, ipnet := range slice {
		encodeIPNet(w, elOid, ipnet)
	}

	return nil
}

func encodeIPSlice(w *WriteBuf, oid Oid, slice []net.IP) error {
	var elOid Oid
	switch oid {
	case InetArrayOid:
		elOid = InetOid
	case CidrArrayOid:
		elOid = CidrOid
	default:
		return fmt.Errorf("cannot encode Go %s into oid %d", "[]net.IPNet", oid)
	}

	size := int32(20) // array header size
	for _, ip := range slice {
		size += 4 + 4 + int32(len(ip)) // size of element + inet/cidr metadata + IP bytes
	}
	w.WriteInt32(int32(size))

	w.WriteInt32(1)                 // number of dimensions
	w.WriteInt32(0)                 // no nulls
	w.WriteInt32(int32(elOid))      // type of elements
	w.WriteInt32(int32(len(slice))) // number of elements
	w.WriteInt32(1)                 // index of first element

	for _, ip := range slice {
		encodeIP(w, elOid, ip)
	}

	return nil
}

func encodeArrayHeader(w *WriteBuf, oid, length, sizePerItem int) {
	w.WriteInt32(int32(20 + length*sizePerItem))
	w.WriteInt32(1)             // number of dimensions
	w.WriteInt32(0)             // no nulls
	w.WriteInt32(int32(oid))    // type of elements
	w.WriteInt32(int32(length)) // number of elements
	w.WriteInt32(1)             // index of first element
}
