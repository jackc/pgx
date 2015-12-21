package pgx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
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
	InetOid             = 869
	BoolArrayOid        = 1000
	Int2ArrayOid        = 1005
	Int4ArrayOid        = 1007
	TextArrayOid        = 1009
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
	UuidOid             = 2950
	JsonbOid            = 3802
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

// DefaultTypeFormats maps type names to their default requested format (text
// or binary). In theory the Scanner interface should be the one to determine
// the format of the returned values. However, the query has already been
// executed by the time Scan is called so it has no chance to set the format.
// So for types that should be returned in binary th
var DefaultTypeFormats map[string]int16

func init() {
	DefaultTypeFormats = map[string]int16{
		"_bool":        BinaryFormatCode,
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
		"timestamp":    BinaryFormatCode,
		"timestamptz":  BinaryFormatCode,
	}
}

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

	return encodeFloat4(w, n.Float32)
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
		return SerializationError(fmt.Sprintf("NullFloat64.EncodeBinary cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeFloat8(w, n.Float64)
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

	return encodeText(w, s.String)
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

	return encodeInt2(w, n.Int16)
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

	return encodeInt4(w, n.Int32)
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

	return encodeInt8(w, n.Int64)
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

	return encodeBool(w, n.Bool)
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

	switch oid {
	case TimestampTzOid:
		return encodeTimestampTz(w, n.Time)
	case TimestampOid:
		return encodeTimestamp(w, n.Time)
	case DateOid:
		return encodeDate(w, n.Time)
	default:
		panic("unreachable")
	}
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

func encodeBool(w *WriteBuf, value interface{}) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("Expected bool, received %T", value)
	}

	w.WriteInt32(1)

	var n byte
	if v {
		n = 1
	}

	w.WriteByte(n)

	return nil
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

func encodeInt8(w *WriteBuf, value interface{}) error {
	var v int64
	switch value := value.(type) {
	case int8:
		v = int64(value)
	case uint8:
		v = int64(value)
	case int16:
		v = int64(value)
	case uint16:
		v = int64(value)
	case int32:
		v = int64(value)
	case uint32:
		v = int64(value)
	case int64:
		v = int64(value)
	case uint64:
		if value > math.MaxInt64 {
			return fmt.Errorf("uint64 %d is larger than max int64 %d", value, int64(math.MaxInt64))
		}
		v = int64(value)
	case int:
		v = int64(value)
	default:
		return fmt.Errorf("Expected integer representable in int64, received %T %v", value, value)
	}

	w.WriteInt32(8)
	w.WriteInt64(v)

	return nil
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

func encodeInt2(w *WriteBuf, value interface{}) error {
	var v int16
	switch value := value.(type) {
	case int8:
		v = int16(value)
	case uint8:
		v = int16(value)
	case int16:
		v = int16(value)
	case uint16:
		if value > math.MaxInt16 {
			return fmt.Errorf("%T %d is larger than max int16 %d", value, value, math.MaxInt16)
		}
		v = int16(value)
	case int32:
		if value > math.MaxInt16 {
			return fmt.Errorf("%T %d is larger than max int16 %d", value, value, math.MaxInt16)
		}
		v = int16(value)
	case uint32:
		if value > math.MaxInt16 {
			return fmt.Errorf("%T %d is larger than max int16 %d", value, value, math.MaxInt16)
		}
		v = int16(value)
	case int64:
		if value > math.MaxInt16 {
			return fmt.Errorf("%T %d is larger than max int16 %d", value, value, math.MaxInt16)
		}
		v = int16(value)
	case uint64:
		if value > math.MaxInt16 {
			return fmt.Errorf("%T %d is larger than max int16 %d", value, value, math.MaxInt16)
		}
		v = int16(value)
	case int:
		if value > math.MaxInt16 {
			return fmt.Errorf("%T %d is larger than max int16 %d", value, value, math.MaxInt16)
		}
		v = int16(value)
	default:
		return fmt.Errorf("Expected integer representable in int16, received %T %v", value, value)
	}

	w.WriteInt32(2)
	w.WriteInt16(v)

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

func encodeInt4(w *WriteBuf, value interface{}) error {
	var v int32
	switch value := value.(type) {
	case int8:
		v = int32(value)
	case uint8:
		v = int32(value)
	case int16:
		v = int32(value)
	case uint16:
		v = int32(value)
	case int32:
		v = int32(value)
	case uint32:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int32 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	case int64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int32 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	case uint64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int32 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	case int:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int32 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	default:
		return fmt.Errorf("Expected integer representable in int32, received %T %v", value, value)
	}

	w.WriteInt32(4)
	w.WriteInt32(v)

	return nil
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

func encodeOid(w *WriteBuf, value interface{}) error {
	v, ok := value.(Oid)
	if !ok {
		return fmt.Errorf("Expected Oid, received %T", value)
	}

	w.WriteInt32(4)
	w.WriteInt32(int32(v))

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

func encodeFloat4(w *WriteBuf, value interface{}) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("Expected float32, received %T", value)
	}

	w.WriteInt32(4)

	w.WriteInt32(int32(math.Float32bits(v)))

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

func encodeFloat8(w *WriteBuf, value interface{}) error {
	var v float64
	switch value := value.(type) {
	case float32:
		v = float64(value)
	case float64:
		v = float64(value)
	default:
		return fmt.Errorf("Expected float representable in float64, received %T %v", value, value)
	}

	w.WriteInt32(8)

	w.WriteInt64(int64(math.Float64bits(v)))

	return nil
}

func decodeText(vr *ValueReader) string {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into string"))
		return ""
	}

	return vr.ReadString(vr.Len())
}

func encodeText(w *WriteBuf, value interface{}) error {
	switch t := value.(type) {
	case string:
		w.WriteInt32(int32(len(t)))
		w.WriteBytes([]byte(t))
	case []byte:
		w.WriteInt32(int32(len(t)))
		w.WriteBytes(t)
	default:
		return fmt.Errorf("Expected string, received %T", value)
	}

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

func encodeBytea(w *WriteBuf, value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("Expected []byte, received %T", value)
	}

	w.WriteInt32(int32(len(b)))
	w.WriteBytes(b)

	return nil
}

func decodeJson(vr *ValueReader, d interface{}) error {
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

func encodeJson(w *WriteBuf, value interface{}) error {
	s, err := json.Marshal(value)
	if err != nil {
		fmt.Errorf("Failed to encode json from type: %T", value)
	}

	return encodeText(w, s)
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

func encodeDate(w *WriteBuf, value interface{}) error {
	t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("Expected time.Time, received %T", value)
	}

	tUnix := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix()
	dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	secSinceDateEpoch := tUnix - dateEpoch
	daysSinceDateEpoch := secSinceDateEpoch / 86400

	w.WriteInt32(4)
	w.WriteInt32(int32(daysSinceDateEpoch))

	return nil
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

func encodeTimestampTz(w *WriteBuf, value interface{}) error {
	t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("Expected time.Time, received %T", value)
	}

	microsecSinceUnixEpoch := t.Unix()*1000000 + int64(t.Nanosecond())/1000
	microsecSinceY2K := microsecSinceUnixEpoch - microsecFromUnixEpochToY2K

	w.WriteInt32(8)
	w.WriteInt64(microsecSinceY2K)

	return nil
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

func encodeTimestamp(w *WriteBuf, value interface{}) error {
	return encodeTimestampTz(w, value)
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
	if vr.Len() != 8 && vr.Len() != 20 {
		vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for a %s: %d", pgType.Name, vr.Len())))
		return zero
	}

	if pgType.DataType != InetOid && pgType.DataType != CidrOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into %s", pgType.DataType, pgType.Name)))
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

func encodeInet(w *WriteBuf, value interface{}) error {
	var ipnet net.IPNet

	switch value := value.(type) {
	case net.IPNet:
		ipnet = value
	case net.IP:
		ipnet.IP = value
		bitCount := len(value) * 8
		ipnet.Mask = net.CIDRMask(bitCount, bitCount)
	default:
		return fmt.Errorf("Expected net.IPNet, received %T %v", value, value)
	}

	var size int32
	var family byte
	switch len(ipnet.IP) {
	case net.IPv4len:
		size = 8
		family = w.conn.pgsql_af_inet
	case net.IPv6len:
		size = 20
		family = w.conn.pgsql_af_inet6
	default:
		return fmt.Errorf("Unexpected IP length: %v", len(ipnet.IP))
	}

	w.WriteInt32(size)
	w.WriteByte(family)
	ones, _ := ipnet.Mask.Size()
	w.WriteByte(byte(ones))
	w.WriteByte(0) // is_cidr is ignored on server
	w.WriteByte(byte(len(ipnet.IP)))
	w.WriteBytes(ipnet.IP)

	return nil
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

func encodeBoolArray(w *WriteBuf, value interface{}) error {
	slice, ok := value.([]bool)
	if !ok {
		return fmt.Errorf("Expected []bool, received %T", value)
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

func encodeInt2Array(w *WriteBuf, value interface{}) error {
	slice, ok := value.([]int16)
	if !ok {
		return fmt.Errorf("Expected []int16, received %T", value)
	}

	encodeArrayHeader(w, Int2Oid, len(slice), 6)
	for _, v := range slice {
		w.WriteInt32(2)
		w.WriteInt16(v)
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

func encodeInt4Array(w *WriteBuf, value interface{}) error {
	slice, ok := value.([]int32)
	if !ok {
		return fmt.Errorf("Expected []int32, received %T", value)
	}

	encodeArrayHeader(w, Int4Oid, len(slice), 8)
	for _, v := range slice {
		w.WriteInt32(4)
		w.WriteInt32(v)
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

func encodeInt8Array(w *WriteBuf, value interface{}) error {
	slice, ok := value.([]int64)
	if !ok {
		return fmt.Errorf("Expected []int64, received %T", value)
	}

	encodeArrayHeader(w, Int8Oid, len(slice), 12)
	for _, v := range slice {
		w.WriteInt32(8)
		w.WriteInt64(v)
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

func encodeFloat4Array(w *WriteBuf, value interface{}) error {
	slice, ok := value.([]float32)
	if !ok {
		return fmt.Errorf("Expected []float32, received %T", value)
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

func encodeFloat8Array(w *WriteBuf, value interface{}) error {
	slice, ok := value.([]float64)
	if !ok {
		return fmt.Errorf("Expected []float64, received %T", value)
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

func encodeTextArray(w *WriteBuf, value interface{}, elOid Oid) error {
	slice, ok := value.([]string)
	if !ok {
		return fmt.Errorf("Expected []string, received %T", value)
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

func encodeTimestampArray(w *WriteBuf, value interface{}, elOid Oid) error {
	slice, ok := value.([]time.Time)
	if !ok {
		return fmt.Errorf("Expected []time.Time, received %T", value)
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

func encodeInetArray(w *WriteBuf, value interface{}, elOid Oid) error {
	slice, ok := value.([]net.IPNet)
	if !ok {
		return fmt.Errorf("Expected []net.IPNet, received %T", value)
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
		encodeInet(w, ipnet)
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
