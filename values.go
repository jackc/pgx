package pgx

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgtype"
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
// So for types that should always be returned in binary the format should be
// set here.
var DefaultTypeFormats map[string]int16

// internalNativeGoTypeFormats lists the encoding type for native Go types (not handled with Encoder interface)
var internalNativeGoTypeFormats map[Oid]int16

func init() {
	DefaultTypeFormats = map[string]int16{
		"_aclitem":     TextFormatCode, // Pg's src/backend/utils/adt/acl.c has only in/out (text) not send/recv (bin)
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
		"aclitem":      TextFormatCode, // Pg's src/backend/utils/adt/acl.c has only in/out (text) not send/recv (bin)
		"bool":         BinaryFormatCode,
		"bytea":        BinaryFormatCode,
		"char":         BinaryFormatCode,
		"cid":          BinaryFormatCode,
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
		"tid":          BinaryFormatCode,
		"timestamp":    BinaryFormatCode,
		"timestamptz":  BinaryFormatCode,
		"xid":          BinaryFormatCode,
	}

	internalNativeGoTypeFormats = map[Oid]int16{
		BoolArrayOid:        BinaryFormatCode,
		BoolOid:             BinaryFormatCode,
		ByteaArrayOid:       BinaryFormatCode,
		ByteaOid:            BinaryFormatCode,
		CidrArrayOid:        BinaryFormatCode,
		CidrOid:             BinaryFormatCode,
		DateOid:             BinaryFormatCode,
		Float4ArrayOid:      BinaryFormatCode,
		Float4Oid:           BinaryFormatCode,
		Float8ArrayOid:      BinaryFormatCode,
		Float8Oid:           BinaryFormatCode,
		InetArrayOid:        BinaryFormatCode,
		InetOid:             BinaryFormatCode,
		Int2ArrayOid:        BinaryFormatCode,
		Int2Oid:             BinaryFormatCode,
		Int4ArrayOid:        BinaryFormatCode,
		Int4Oid:             BinaryFormatCode,
		Int8ArrayOid:        BinaryFormatCode,
		Int8Oid:             BinaryFormatCode,
		JsonbOid:            BinaryFormatCode,
		JsonOid:             BinaryFormatCode,
		OidOid:              BinaryFormatCode,
		RecordOid:           BinaryFormatCode,
		TextArrayOid:        BinaryFormatCode,
		TimestampArrayOid:   BinaryFormatCode,
		TimestampOid:        BinaryFormatCode,
		TimestampTzArrayOid: BinaryFormatCode,
		TimestampTzOid:      BinaryFormatCode,
		VarcharArrayOid:     BinaryFormatCode,
	}
}

// SerializationError occurs on failure to encode or decode a value
type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

// Deprecated: Scanner is an interface used to decode values from the PostgreSQL
// server. To allow types to support pgx and database/sql.Scan this interface
// has been deprecated in favor of PgxScanner.
type Scanner interface {
	// Scan MUST check r.Type().DataType (to check by Oid) or
	// r.Type().DataTypeName (to check by name) to ensure that it is scanning an
	// expected column type. It also MUST check r.Type().FormatCode before
	// decoding. It should not assume that it was called on a data type or format
	// that it understands.
	Scan(r *ValueReader) error
}

// PgxScanner is an interface used to decode values from the PostgreSQL server.
// It is used exactly the same as the Scanner interface. It simply has renamed
// the method.
type PgxScanner interface {
	// ScanPgx MUST check r.Type().DataType (to check by Oid) or
	// r.Type().DataTypeName (to check by name) to ensure that it is scanning an
	// expected column type. It also MUST check r.Type().FormatCode before
	// decoding. It should not assume that it was called on a data type or format
	// that it understands.
	ScanPgx(r *ValueReader) error
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
		return SerializationError(fmt.Sprintf("NullFloat32.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullFloat32.Encode cannot encode into Oid %d", oid))
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
		return SerializationError(fmt.Sprintf("NullFloat64.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullFloat64.Encode cannot encode into Oid %d", oid))
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

func (n *NullString) Scan(vr *ValueReader) error {
	// Not checking oid as so we can scan anything into into a NullString - may revisit this decision later

	if vr.Len() == -1 {
		n.String, n.Valid = "", false
		return nil
	}

	n.Valid = true
	n.String = decodeText(vr)
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

// NullInt16 represents a smallint that may be null. NullInt16 implements the
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
		return SerializationError(fmt.Sprintf("NullInt16.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullInt16.Encode cannot encode into Oid %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	w.WriteInt32(2)

	_, err := pgtype.Int2{Int: n.Int16, Status: pgtype.Present}.EncodeBinary(w)
	return err
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
		return SerializationError(fmt.Sprintf("NullInt32.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullInt32.Encode cannot encode into Oid %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	w.WriteInt32(4)

	_, err := pgtype.Int4{Int: n.Int32, Status: pgtype.Present}.EncodeBinary(w)
	return err
}

// Oid (Object Identifier Type) is, according to https://www.postgresql.org/docs/current/static/datatype-oid.html,
// used internally by PostgreSQL as a primary key for various system tables. It is currently implemented
// as an unsigned four-byte integer. Its definition can be found in src/include/postgres_ext.h
// in the PostgreSQL sources. Oid cannot be NULL. To allow for NULL Oids use pgtype.Oid.
type Oid uint32

func (dst *Oid) DecodeText(src []byte) error {
	if src == nil {
		return fmt.Errorf("cannot decode nil into Oid")
	}

	n, err := strconv.ParseUint(string(src), 10, 32)
	if err != nil {
		return err
	}

	*dst = Oid(n)
	return nil
}

func (dst *Oid) DecodeBinary(src []byte) error {
	if src == nil {
		return fmt.Errorf("cannot decode nil into Oid")
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length: %v", len(src))
	}

	n := binary.BigEndian.Uint32(src)
	*dst = Oid(n)
	return nil
}

func (src Oid) EncodeText(w io.Writer) (bool, error) {
	_, err := io.WriteString(w, strconv.FormatUint(uint64(src), 10))
	return false, err
}

func (src Oid) EncodeBinary(w io.Writer) (bool, error) {
	_, err := pgio.WriteUint32(w, uint32(src))
	return false, err
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
		return SerializationError(fmt.Sprintf("NullInt64.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullInt64.Encode cannot encode into Oid %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	w.WriteInt32(8)

	_, err := pgtype.Int8{Int: n.Int64, Status: pgtype.Present}.EncodeBinary(w)
	return err
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
		return SerializationError(fmt.Sprintf("NullBool.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullBool.Encode cannot encode into Oid %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	w.WriteInt32(1)

	_, err := pgtype.Bool{Bool: n.Bool, Status: pgtype.Present}.EncodeBinary(w)
	return err
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
		return SerializationError(fmt.Sprintf("NullTime.Scan cannot decode Oid %d", vr.Type().DataType))
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
		return SerializationError(fmt.Sprintf("NullTime.Encode cannot encode into Oid %d", oid))
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
	case pgtype.BinaryEncoder:
		buf := &bytes.Buffer{}
		null, err := arg.EncodeBinary(buf)
		if err != nil {
			return err
		}
		if null {
			wbuf.WriteInt32(-1)
		} else {
			wbuf.WriteInt32(int32(buf.Len()))
			wbuf.WriteBytes(buf.Bytes())
		}
		return nil
	case pgtype.TextEncoder:
		buf := &bytes.Buffer{}
		null, err := arg.EncodeText(buf)
		if err != nil {
			return err
		}
		if null {
			wbuf.WriteInt32(-1)
		} else {
			wbuf.WriteInt32(int32(buf.Len()))
			wbuf.WriteBytes(buf.Bytes())
		}
		return nil
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
	}

	refVal := reflect.ValueOf(arg)

	if refVal.Kind() == reflect.Ptr {
		if refVal.IsNil() {
			wbuf.WriteInt32(-1)
			return nil
		}
		arg = refVal.Elem().Interface()
		return Encode(wbuf, oid, arg)
	}

	if value, ok := wbuf.conn.oidPgtypeValues[oid]; ok {
		if converterFrom, ok := value.(pgtype.ConverterFrom); ok {
			err := converterFrom.ConvertFrom(arg)
			if err != nil {
				return err
			}
		} else {
			return SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
		}

		buf := &bytes.Buffer{}
		null, err := value.(pgtype.BinaryEncoder).EncodeBinary(buf)
		if err != nil {
			return err
		}
		if null {
			wbuf.WriteInt32(-1)
		} else {
			wbuf.WriteInt32(int32(buf.Len()))
			wbuf.WriteBytes(buf.Bytes())
		}
		return nil
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		return Encode(wbuf, oid, strippedArg)
	}
	return SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
}

func stripNamedType(val *reflect.Value) (interface{}, bool) {
	switch val.Kind() {
	case reflect.Int:
		convVal := int(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int8:
		convVal := int8(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int16:
		convVal := int16(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int32:
		convVal := int32(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int64:
		convVal := int64(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint:
		convVal := uint(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint8:
		convVal := uint8(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint16:
		convVal := uint16(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint32:
		convVal := uint32(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint64:
		convVal := uint64(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.String:
		convVal := val.String()
		return convVal, reflect.TypeOf(convVal) != val.Type()
	}

	return nil, false
}

// Decode decodes from vr into d. d must be a pointer. This allows
// implementations of the Decoder interface to delegate the actual work of
// decoding to the built-in functionality.
func Decode(vr *ValueReader, d interface{}) error {
	switch v := d.(type) {
	case *string:
		*v = decodeText(vr)
	case *[]interface{}:
		*v = decodeRecord(vr)
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
				}
				if el.IsNil() {
					// allocate destination
					el.Set(reflect.New(el.Type().Elem()))
				}
				d = el.Interface()
				return Decode(vr, d)
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
	if vr.Type().DataType != BoolOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into bool", vr.Type().DataType)))
		return false
	}

	var b pgtype.Bool
	var err error
	switch vr.Type().FormatCode {
	case TextFormatCode:
		err = b.DecodeText(vr.bytes())
	case BinaryFormatCode:
		err = b.DecodeBinary(vr.bytes())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return false
	}

	if err != nil {
		vr.Fatal(err)
		return false
	}

	if b.Status != pgtype.Present {
		vr.Fatal(fmt.Errorf("Cannot decode null into bool"))
		return false
	}

	return b.Bool
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

	var n pgtype.Int8
	var err error
	switch vr.Type().FormatCode {
	case TextFormatCode:
		err = n.DecodeText(vr.bytes())
	case BinaryFormatCode:
		err = n.DecodeBinary(vr.bytes())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if err != nil {
		vr.Fatal(err)
		return 0
	}

	if n.Status == pgtype.Null {
		vr.Fatal(ProtocolError("Cannot decode null into int16"))
		return 0
	}

	return n.Int
}

func decodeInt2(vr *ValueReader) int16 {

	if vr.Type().DataType != Int2Oid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into int16", vr.Type().DataType)))
		return 0
	}

	var n pgtype.Int2
	var err error
	switch vr.Type().FormatCode {
	case TextFormatCode:
		err = n.DecodeText(vr.bytes())
	case BinaryFormatCode:
		err = n.DecodeBinary(vr.bytes())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if err != nil {
		vr.Fatal(err)
		return 0
	}

	if n.Status == pgtype.Null {
		vr.Fatal(ProtocolError("Cannot decode null into int16"))
		return 0
	}

	return n.Int
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

	var n pgtype.Int4
	var err error
	switch vr.Type().FormatCode {
	case TextFormatCode:
		err = n.DecodeText(vr.bytes())
	case BinaryFormatCode:
		err = n.DecodeBinary(vr.bytes())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return 0
	}

	if err != nil {
		vr.Fatal(err)
		return 0
	}

	if n.Status == pgtype.Null {
		vr.Fatal(ProtocolError("Cannot decode null into int16"))
		return 0
	}

	return n.Int
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

	if vr.Type().FormatCode == BinaryFormatCode {
		vr.Fatal(ProtocolError("cannot decode binary value into string"))
		return ""
	}

	return vr.ReadString(vr.Len())
}

func decodeTextAllowBinary(vr *ValueReader) string {
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

	if vr.Type().DataType != JsonOid {
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
	if oid != JsonOid {
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

func decodeJSONB(vr *ValueReader, d interface{}) error {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != JsonbOid {
		err := ProtocolError(fmt.Sprintf("Cannot decode oid %v into jsonb", vr.Type().DataType))
		vr.Fatal(err)
		return err
	}

	bytes := vr.ReadBytes(vr.Len())
	if vr.Type().FormatCode == BinaryFormatCode {
		if bytes[0] != 1 {
			err := ProtocolError(fmt.Sprintf("Unknown jsonb format byte: %x", bytes[0]))
			vr.Fatal(err)
			return err
		}
		bytes = bytes[1:]
	}

	err := json.Unmarshal(bytes, d)
	if err != nil {
		vr.Fatal(err)
	}
	return err
}

func encodeJSONB(w *WriteBuf, oid Oid, value interface{}) error {
	if oid != JsonbOid {
		return fmt.Errorf("cannot encode JSON into oid %v", oid)
	}

	s, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Failed to encode json from type: %T", value)
	}

	w.WriteInt32(int32(len(s) + 1))
	w.WriteByte(1) // JSONB format header
	w.WriteBytes(s)

	return nil
}

func decodeDate(vr *ValueReader) time.Time {
	if vr.Type().DataType != DateOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into time.Time", vr.Type().DataType)))
		return time.Time{}
	}

	var d pgtype.Date
	var err error
	switch vr.Type().FormatCode {
	case TextFormatCode:
		err = d.DecodeText(vr.bytes())
	case BinaryFormatCode:
		err = d.DecodeBinary(vr.bytes())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return time.Time{}
	}

	if err != nil {
		vr.Fatal(err)
		return time.Time{}
	}

	if d.Status == pgtype.Null {
		vr.Fatal(ProtocolError("Cannot decode null into int16"))
		return time.Time{}
	}

	return d.Time
}

func encodeTime(w *WriteBuf, oid Oid, value time.Time) error {
	switch oid {
	case DateOid:
		var d pgtype.Date
		err := d.ConvertFrom(value)
		if err != nil {
			return err
		}

		buf := &bytes.Buffer{}
		null, err := d.EncodeBinary(buf)
		if err != nil {
			return err
		}
		if null {
			w.WriteInt32(-1)
		} else {
			w.WriteInt32(int32(buf.Len()))
			w.WriteBytes(buf.Bytes())
		}
		return nil

	case TimestampTzOid, TimestampOid:
		var t pgtype.Timestamptz
		err := t.ConvertFrom(value)
		if err != nil {
			return err
		}

		buf := &bytes.Buffer{}
		null, err := t.EncodeBinary(buf)
		if err != nil {
			return err
		}
		if null {
			w.WriteInt32(-1)
		} else {
			w.WriteInt32(int32(buf.Len()))
			w.WriteBytes(buf.Bytes())
		}
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

	var t pgtype.Timestamptz
	var err error
	switch vr.Type().FormatCode {
	case TextFormatCode:
		err = t.DecodeText(vr.bytes())
	case BinaryFormatCode:
		err = t.DecodeBinary(vr.bytes())
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return time.Time{}
	}

	if err != nil {
		vr.Fatal(err)
		return time.Time{}
	}

	if t.Status == pgtype.Null {
		vr.Fatal(ProtocolError("Cannot decode null into time.Time"))
		return time.Time{}
	}

	return t.Time
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
		case TextOid, VarcharOid, UnknownOid:
			record = append(record, decodeTextAllowBinary(&fieldVR))
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
