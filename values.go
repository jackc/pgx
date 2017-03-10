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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgtype"
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
	TidOID              = 27
	XIDOID              = 28
	CIDOID              = 29
	JSONOID             = 114
	CidrOID             = 650
	CidrArrayOID        = 651
	Float4OID           = 700
	Float8OID           = 701
	UnknownOID          = 705
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
	AclItemOID          = 1033
	AclItemArrayOID     = 1034
	InetArrayOID        = 1041
	VarcharOID          = 1043
	DateOID             = 1082
	TimestampOID        = 1114
	TimestampArrayOID   = 1115
	DateArrayOID        = 1182
	TimestampTzOID      = 1184
	TimestampTzArrayOID = 1185
	RecordOID           = 2249
	UUIDOID             = 2950
	JSONBOID            = 3802
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
var internalNativeGoTypeFormats map[OID]int16

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

	internalNativeGoTypeFormats = map[OID]int16{
		BoolArrayOID:        BinaryFormatCode,
		BoolOID:             BinaryFormatCode,
		ByteaArrayOID:       BinaryFormatCode,
		ByteaOID:            BinaryFormatCode,
		CidrArrayOID:        BinaryFormatCode,
		CidrOID:             BinaryFormatCode,
		DateOID:             BinaryFormatCode,
		Float4ArrayOID:      BinaryFormatCode,
		Float4OID:           BinaryFormatCode,
		Float8ArrayOID:      BinaryFormatCode,
		Float8OID:           BinaryFormatCode,
		InetArrayOID:        BinaryFormatCode,
		InetOID:             BinaryFormatCode,
		Int2ArrayOID:        BinaryFormatCode,
		Int2OID:             BinaryFormatCode,
		Int4ArrayOID:        BinaryFormatCode,
		Int4OID:             BinaryFormatCode,
		Int8ArrayOID:        BinaryFormatCode,
		Int8OID:             BinaryFormatCode,
		JSONBOID:            BinaryFormatCode,
		JSONOID:             BinaryFormatCode,
		OIDOID:              BinaryFormatCode,
		RecordOID:           BinaryFormatCode,
		TextArrayOID:        BinaryFormatCode,
		TimestampArrayOID:   BinaryFormatCode,
		TimestampOID:        BinaryFormatCode,
		TimestampTzArrayOID: BinaryFormatCode,
		TimestampTzOID:      BinaryFormatCode,
		VarcharArrayOID:     BinaryFormatCode,
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
	// Scan MUST check r.Type().DataType (to check by OID) or
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
	// ScanPgx MUST check r.Type().DataType (to check by OID) or
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
	Encode(w *WriteBuf, oid OID) error

	// FormatCode returns the format that the encoder writes the value. It must be
	// either pgx.TextFormatCode or pgx.BinaryFormatCode.
	FormatCode() int16
}

type ScannerV3 interface {
	ScanPgxV3(fieldDescription interface{}, src interface{}) error
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
	if vr.Type().DataType != Float4OID {
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

func (n NullFloat32) Encode(w *WriteBuf, oid OID) error {
	if oid != Float4OID {
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
	if vr.Type().DataType != Float8OID {
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

func (n NullFloat64) Encode(w *WriteBuf, oid OID) error {
	if oid != Float8OID {
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

func (s NullString) Encode(w *WriteBuf, oid OID) error {
	if !s.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeString(w, oid, s.String)
}

// AclItem is used for PostgreSQL's aclitem data type. A sample aclitem
// might look like this:
//
//	postgres=arwdDxt/postgres
//
// Note, however, that because the user/role name part of an aclitem is
// an identifier, it follows all the usual formatting rules for SQL
// identifiers: if it contains spaces and other special characters,
// it should appear in double-quotes:
//
//	postgres=arwdDxt/"role with spaces"
//
type AclItem string

// NullAclItem represents a pgx.AclItem that may be null. NullAclItem implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullAclItem struct {
	AclItem AclItem
	Valid   bool // Valid is true if AclItem is not NULL
}

func (n *NullAclItem) Scan(vr *ValueReader) error {
	if vr.Type().DataType != AclItemOID {
		return SerializationError(fmt.Sprintf("NullAclItem.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.AclItem, n.Valid = "", false
		return nil
	}

	n.Valid = true
	n.AclItem = AclItem(decodeText(vr))
	return vr.Err()
}

// Particularly important to return TextFormatCode, seeing as Postgres
// only ever sends aclitem as text, not binary.
func (n NullAclItem) FormatCode() int16 { return TextFormatCode }

func (n NullAclItem) Encode(w *WriteBuf, oid OID) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeString(w, oid, string(n.AclItem))
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
	if vr.Type().DataType != Int2OID {
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

func (n NullInt16) Encode(w *WriteBuf, oid OID) error {
	if oid != Int2OID {
		return SerializationError(fmt.Sprintf("NullInt16.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return pgtype.Int2{Int: n.Int16, Status: pgtype.Present}.EncodeBinary(w)
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
	if vr.Type().DataType != Int4OID {
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

func (n NullInt32) Encode(w *WriteBuf, oid OID) error {
	if oid != Int4OID {
		return SerializationError(fmt.Sprintf("NullInt32.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return pgtype.Int4{Int: n.Int32, Status: pgtype.Present}.EncodeBinary(w)
}

// OID (Object Identifier Type) is, according to https://www.postgresql.org/docs/current/static/datatype-oid.html,
// used internally by PostgreSQL as a primary key for various system tables. It is currently implemented
// as an unsigned four-byte integer. Its definition can be found in src/include/postgres_ext.h
// in the PostgreSQL sources. OID cannot be NULL. To allow for NULL OIDs use pgtype.OID.
type OID uint32

func (dst *OID) DecodeText(src []byte) error {
	if src == nil {
		return fmt.Errorf("cannot decode nil into OID")
	}

	n, err := strconv.ParseUint(string(src), 10, 32)
	if err != nil {
		return err
	}

	*dst = OID(n)
	return nil
}

func (dst *OID) DecodeBinary(src []byte) error {
	if src == nil {
		return fmt.Errorf("cannot decode nil into OID")
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length: %v", len(src))
	}

	n := binary.BigEndian.Uint32(src)
	*dst = OID(n)
	return nil
}

func (src OID) EncodeText(w io.Writer) error {
	s := strconv.FormatUint(uint64(src), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (src OID) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	_, err = pgio.WriteUint32(w, uint32(src))
	return err
}

// Tid is PostgreSQL's Tuple Identifier type.
//
// When one does
//
// 	select ctid, * from some_table;
//
// it is the data type of the ctid hidden system column.
//
// It is currently implemented as a pair unsigned two byte integers.
// Its conversion functions can be found in src/backend/utils/adt/tid.c
// in the PostgreSQL sources.
type Tid struct {
	BlockNumber  uint32
	OffsetNumber uint16
}

// NullTid represents a Tuple Identifier (Tid) that may be null. NullTid implements the
// Scanner and Encoder interfaces so it may be used both as an argument to
// Query[Row] and a destination for Scan.
//
// If Valid is false then the value is NULL.
type NullTid struct {
	Tid   Tid
	Valid bool // Valid is true if Tid is not NULL
}

func (n *NullTid) Scan(vr *ValueReader) error {
	if vr.Type().DataType != TidOID {
		return SerializationError(fmt.Sprintf("NullTid.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Tid, n.Valid = Tid{BlockNumber: 0, OffsetNumber: 0}, false
		return nil
	}
	n.Valid = true
	n.Tid = decodeTid(vr)
	return vr.Err()
}

func (n NullTid) FormatCode() int16 { return BinaryFormatCode }

func (n NullTid) Encode(w *WriteBuf, oid OID) error {
	if oid != TidOID {
		return SerializationError(fmt.Sprintf("NullTid.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeTid(w, oid, n.Tid)
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
	if vr.Type().DataType != Int8OID {
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

func (n NullInt64) Encode(w *WriteBuf, oid OID) error {
	if oid != Int8OID {
		return SerializationError(fmt.Sprintf("NullInt64.Encode cannot encode into OID %d", oid))
	}

	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return pgtype.Int8{Int: n.Int64, Status: pgtype.Present}.EncodeBinary(w)
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
	if vr.Type().DataType != BoolOID {
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

func (n NullBool) Encode(w *WriteBuf, oid OID) error {
	if oid != BoolOID {
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
	if oid != TimestampTzOID && oid != TimestampOID && oid != DateOID {
		return SerializationError(fmt.Sprintf("NullTime.Scan cannot decode OID %d", vr.Type().DataType))
	}

	if vr.Len() == -1 {
		n.Time, n.Valid = time.Time{}, false
		return nil
	}

	n.Valid = true
	switch oid {
	case TimestampTzOID:
		n.Time = decodeTimestampTz(vr)
	case TimestampOID:
		n.Time = decodeTimestamp(vr)
	case DateOID:
		n.Time = decodeDate(vr)
	}

	return vr.Err()
}

func (n NullTime) FormatCode() int16 { return BinaryFormatCode }

func (n NullTime) Encode(w *WriteBuf, oid OID) error {
	if oid != TimestampTzOID && oid != TimestampOID && oid != DateOID {
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

func (h Hstore) Encode(w *WriteBuf, oid OID) error {
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

func (h NullHstore) Encode(w *WriteBuf, oid OID) error {
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
func Encode(wbuf *WriteBuf, oid OID, arg interface{}) error {
	if arg == nil {
		wbuf.WriteInt32(-1)
		return nil
	}

	switch arg := arg.(type) {
	case Encoder:
		return arg.Encode(wbuf, oid)
	case pgtype.BinaryEncoder:
		return arg.EncodeBinary(wbuf)
	case pgtype.TextEncoder:
		return arg.EncodeText(wbuf)
	case driver.Valuer:
		v, err := arg.Value()
		if err != nil {
			return err
		}
		return Encode(wbuf, oid, v)
	case string:
		return encodeString(wbuf, oid, arg)
	case []AclItem:
		return encodeAclItemSlice(wbuf, oid, arg)
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
		}
		arg = refVal.Elem().Interface()
		return Encode(wbuf, oid, arg)
	}

	if oid == JSONOID {
		return encodeJSON(wbuf, oid, arg)
	}
	if oid == JSONBOID {
		return encodeJSONB(wbuf, oid, arg)
	}

	if value, ok := wbuf.conn.oidPgtypeValues[oid]; ok {
		err := value.ConvertFrom(arg)
		if err != nil {
			return err
		}
		return value.(pgtype.BinaryEncoder).EncodeBinary(wbuf)
	}

	switch arg := arg.(type) {
	case AclItem:
		// The aclitem data type goes over the wire using the same format as string,
		// so just cast to string and use encodeString
		return encodeString(wbuf, oid, string(arg))
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

func decodeByOID(vr *ValueReader) (interface{}, error) {
	switch vr.Type().DataType {
	case Int2OID, Int4OID, Int8OID:
		n := decodeInt(vr)
		return n, vr.Err()
	case BoolOID:
		b := decodeBool(vr)
		return b, vr.Err()
	default:
		buf := vr.ReadBytes(vr.Len())
		return buf, vr.Err()
	}
}

// Decode decodes from vr into d. d must be a pointer. This allows
// implementations of the Decoder interface to delegate the actual work of
// decoding to the built-in functionality.
func Decode(vr *ValueReader, d interface{}) error {
	switch v := d.(type) {
	case *AclItem:
		// aclitem goes over the wire just like text
		*v = AclItem(decodeText(vr))
	case *Tid:
		*v = decodeTid(vr)
	case *string:
		*v = decodeText(vr)
	case *[]AclItem:
		*v = decodeAclItemArray(vr)
	case *[][]byte:
		*v = decodeByteaArray(vr)
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
	if vr.Type().DataType != BoolOID {
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

func encodeBool(w *WriteBuf, oid OID, value bool) error {
	if oid != BoolOID {
		return fmt.Errorf("cannot encode Go %s into oid %d", "bool", oid)
	}

	b := pgtype.Bool{Bool: value, Status: pgtype.Present}
	return b.EncodeBinary(w)
}

func decodeInt(vr *ValueReader) int64 {
	switch vr.Type().DataType {
	case Int2OID:
		return int64(decodeInt2(vr))
	case Int4OID:
		return int64(decodeInt4(vr))
	case Int8OID:
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

	if vr.Type().DataType != Int8OID {
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

	if vr.Type().DataType != Int2OID {
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

	if vr.Type().DataType != Int4OID {
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

// Note that we do not match negative numbers, because neither the
// BlockNumber nor OffsetNumber of a Tid can be negative.
var tidRegexp *regexp.Regexp = regexp.MustCompile(`^\((\d*),(\d*)\)$`)

func decodeTid(vr *ValueReader) Tid {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into Tid"))
		return Tid{BlockNumber: 0, OffsetNumber: 0}
	}

	if vr.Type().DataType != TidOID {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into pgx.Tid", vr.Type().DataType)))
		return Tid{BlockNumber: 0, OffsetNumber: 0}
	}

	// Unlikely Tid will ever go over the wire as text format, but who knows?
	switch vr.Type().FormatCode {
	case TextFormatCode:
		s := vr.ReadString(vr.Len())

		match := tidRegexp.FindStringSubmatch(s)
		if match == nil {
			vr.Fatal(ProtocolError(fmt.Sprintf("Received invalid OID: %v", s)))
			return Tid{BlockNumber: 0, OffsetNumber: 0}
		}

		blockNumber, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			vr.Fatal(ProtocolError(fmt.Sprintf("Received invalid BlockNumber part of a Tid: %v", s)))
		}

		offsetNumber, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			vr.Fatal(ProtocolError(fmt.Sprintf("Received invalid offsetNumber part of a Tid: %v", s)))
		}
		return Tid{BlockNumber: uint32(blockNumber), OffsetNumber: uint16(offsetNumber)}
	case BinaryFormatCode:
		if vr.Len() != 6 {
			vr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an OID: %d", vr.Len())))
			return Tid{BlockNumber: 0, OffsetNumber: 0}
		}
		return Tid{BlockNumber: vr.ReadUint32(), OffsetNumber: vr.ReadUint16()}
	default:
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return Tid{BlockNumber: 0, OffsetNumber: 0}
	}
}

func encodeTid(w *WriteBuf, oid OID, value Tid) error {
	if oid != TidOID {
		return fmt.Errorf("cannot encode Go %s into oid %d", "pgx.Tid", oid)
	}

	w.WriteInt32(6)
	w.WriteUint32(value.BlockNumber)
	w.WriteUint16(value.OffsetNumber)

	return nil
}

func decodeFloat4(vr *ValueReader) float32 {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into float32"))
		return 0
	}

	if vr.Type().DataType != Float4OID {
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

func encodeFloat32(w *WriteBuf, oid OID, value float32) error {
	switch oid {
	case Float4OID:
		w.WriteInt32(4)
		w.WriteInt32(int32(math.Float32bits(value)))
	case Float8OID:
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

	if vr.Type().DataType != Float8OID {
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

func encodeFloat64(w *WriteBuf, oid OID, value float64) error {
	switch oid {
	case Float8OID:
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

func encodeString(w *WriteBuf, oid OID, value string) error {
	w.WriteInt32(int32(len(value)))
	w.WriteBytes([]byte(value))
	return nil
}

func decodeBytea(vr *ValueReader) []byte {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != ByteaOID {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []byte", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	return vr.ReadBytes(vr.Len())
}

func encodeByteSlice(w *WriteBuf, oid OID, value []byte) error {
	w.WriteInt32(int32(len(value)))
	w.WriteBytes(value)

	return nil
}

func decodeJSON(vr *ValueReader, d interface{}) error {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != JSONOID {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into json", vr.Type().DataType)))
	}

	bytes := vr.ReadBytes(vr.Len())
	err := json.Unmarshal(bytes, d)
	if err != nil {
		vr.Fatal(err)
	}
	return err
}

func encodeJSON(w *WriteBuf, oid OID, value interface{}) error {
	if oid != JSONOID {
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

	if vr.Type().DataType != JSONBOID {
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

func encodeJSONB(w *WriteBuf, oid OID, value interface{}) error {
	if oid != JSONBOID {
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
	if vr.Type().DataType != DateOID {
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

func encodeTime(w *WriteBuf, oid OID, value time.Time) error {
	switch oid {
	case DateOID:
		var d pgtype.Date
		err := d.ConvertFrom(value)
		if err != nil {
			return err
		}
		return d.EncodeBinary(w)
	case TimestampTzOID, TimestampOID:
		var t pgtype.Timestamptz
		err := t.ConvertFrom(value)
		if err != nil {
			return err
		}
		return t.EncodeBinary(w)
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

	if vr.Type().DataType != TimestampTzOID {
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

	if vr.Type().DataType != TimestampOID {
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

	if vr.Type().DataType != RecordOID {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []interface{}", vr.Type().DataType)))
		return nil
	}

	valueCount := vr.ReadInt32()
	record := make([]interface{}, 0, int(valueCount))

	for i := int32(0); i < valueCount; i++ {
		fd := FieldDescription{FormatCode: BinaryFormatCode}
		fieldVR := ValueReader{mr: vr.mr, fd: &fd}
		fd.DataType = vr.ReadOID()
		fieldVR.valueBytesRemaining = vr.ReadInt32()
		vr.valueBytesRemaining -= fieldVR.valueBytesRemaining

		switch fd.DataType {
		case BoolOID:
			record = append(record, decodeBool(&fieldVR))
		case ByteaOID:
			record = append(record, decodeBytea(&fieldVR))
		case Int8OID:
			record = append(record, decodeInt8(&fieldVR))
		case Int2OID:
			record = append(record, decodeInt2(&fieldVR))
		case Int4OID:
			record = append(record, decodeInt4(&fieldVR))
		case Float4OID:
			record = append(record, decodeFloat4(&fieldVR))
		case Float8OID:
			record = append(record, decodeFloat8(&fieldVR))
		case DateOID:
			record = append(record, decodeDate(&fieldVR))
		case TimestampTzOID:
			record = append(record, decodeTimestampTz(&fieldVR))
		case TimestampOID:
			record = append(record, decodeTimestamp(&fieldVR))
		case TextOID, VarcharOID, UnknownOID:
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

func decodeByteaArray(vr *ValueReader) [][]byte {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != ByteaArrayOID {
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

func encodeByteSliceSlice(w *WriteBuf, oid OID, value [][]byte) error {
	if oid != ByteaArrayOID {
		return fmt.Errorf("cannot encode Go %s into oid %d", "[][]byte", oid)
	}

	size := 20 // array header size
	for _, el := range value {
		size += 4 + len(el)
	}

	w.WriteInt32(int32(size))

	w.WriteInt32(1)                 // number of dimensions
	w.WriteInt32(0)                 // no nulls
	w.WriteInt32(int32(ByteaOID))   // type of elements
	w.WriteInt32(int32(len(value))) // number of elements
	w.WriteInt32(1)                 // index of first element

	for _, el := range value {
		encodeByteSlice(w, ByteaOID, el)
	}

	return nil
}

// escapeAclItem escapes an AclItem before it is added to
// its aclitem[] string representation. The PostgreSQL aclitem
// datatype itself can need escapes because it follows the
// formatting rules of SQL identifiers. Think of this function
// as escaping the escapes, so that PostgreSQL's array parser
// will do the right thing.
func escapeAclItem(acl string) (string, error) {
	var escapedAclItem bytes.Buffer
	reader := strings.NewReader(acl)
	for {
		rn, _, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				// Here, EOF is an expected end state, not an error.
				return escapedAclItem.String(), nil
			}
			// This error was not expected
			return "", err
		}
		if needsEscape(rn) {
			escapedAclItem.WriteRune('\\')
		}
		escapedAclItem.WriteRune(rn)
	}
}

// needsEscape determines whether or not a rune needs escaping
// before being placed in the textual representation of an
// aclitem[] array.
func needsEscape(rn rune) bool {
	return rn == '\\' || rn == ',' || rn == '"' || rn == '}'
}

// encodeAclItemSlice encodes a slice of AclItems in
// their textual represention for PostgreSQL.
func encodeAclItemSlice(w *WriteBuf, oid OID, aclitems []AclItem) error {
	strs := make([]string, len(aclitems))
	var escapedAclItem string
	var err error
	for i := range strs {
		escapedAclItem, err = escapeAclItem(string(aclitems[i]))
		if err != nil {
			return err
		}
		strs[i] = string(escapedAclItem)
	}

	var buf bytes.Buffer
	buf.WriteRune('{')
	buf.WriteString(strings.Join(strs, ","))
	buf.WriteRune('}')
	str := buf.String()
	w.WriteInt32(int32(len(str)))
	w.WriteBytes([]byte(str))
	return nil
}

// parseAclItemArray parses the textual representation
// of the aclitem[] type. The textual representation is chosen because
// Pg's src/backend/utils/adt/acl.c has only in/out (text) not send/recv (bin).
// See https://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
// for formatting notes.
func parseAclItemArray(arr string) ([]AclItem, error) {
	reader := strings.NewReader(arr)
	// Difficult to guess a performant initial capacity for a slice of
	// aclitems, but let's go with 5.
	aclItems := make([]AclItem, 0, 5)
	// A single value
	aclItem := AclItem("")
	for {
		// Grab the first/next/last rune to see if we are dealing with a
		// quoted value, an unquoted value, or the end of the string.
		rn, _, err := reader.ReadRune()
		if err != nil {
			if err == io.EOF {
				// Here, EOF is an expected end state, not an error.
				return aclItems, nil
			}
			// This error was not expected
			return nil, err
		}

		if rn == '"' {
			// Discard the opening quote of the quoted value.
			aclItem, err = parseQuotedAclItem(reader)
		} else {
			// We have just read the first rune of an unquoted (bare) value;
			// put it back so that ParseBareValue can read it.
			err := reader.UnreadRune()
			if err != nil {
				return nil, err
			}
			aclItem, err = parseBareAclItem(reader)
		}

		if err != nil {
			if err == io.EOF {
				// Here, EOF is an expected end state, not an error..
				aclItems = append(aclItems, aclItem)
				return aclItems, nil
			}
			// This error was not expected.
			return nil, err
		}
		aclItems = append(aclItems, aclItem)
	}
}

// parseBareAclItem parses a bare (unquoted) aclitem from reader
func parseBareAclItem(reader *strings.Reader) (AclItem, error) {
	var aclItem bytes.Buffer
	for {
		rn, _, err := reader.ReadRune()
		if err != nil {
			// Return the read value in case the error is a harmless io.EOF.
			// (io.EOF marks the end of a bare aclitem at the end of a string)
			return AclItem(aclItem.String()), err
		}
		if rn == ',' {
			// A comma marks the end of a bare aclitem.
			return AclItem(aclItem.String()), nil
		} else {
			aclItem.WriteRune(rn)
		}
	}
}

// parseQuotedAclItem parses an aclitem which is in double quotes from reader
func parseQuotedAclItem(reader *strings.Reader) (AclItem, error) {
	var aclItem bytes.Buffer
	for {
		rn, escaped, err := readPossiblyEscapedRune(reader)
		if err != nil {
			if err == io.EOF {
				// Even when it is the last value, the final rune of
				// a quoted aclitem should be the final closing quote, not io.EOF.
				return AclItem(""), fmt.Errorf("unexpected end of quoted value")
			}
			// Return the read aclitem in case the error is a harmless io.EOF,
			// which will be determined by the caller.
			return AclItem(aclItem.String()), err
		}
		if !escaped && rn == '"' {
			// An unescaped double quote marks the end of a quoted value.
			// The next rune should either be a comma or the end of the string.
			rn, _, err := reader.ReadRune()
			if err != nil {
				// Return the read value in case the error is a harmless io.EOF,
				// which will be determined by the caller.
				return AclItem(aclItem.String()), err
			}
			if rn != ',' {
				return AclItem(""), fmt.Errorf("unexpected rune after quoted value")
			}
			return AclItem(aclItem.String()), nil
		}
		aclItem.WriteRune(rn)
	}
}

// Returns the next rune from r, unless it is a backslash;
// in that case, it returns the rune after the backslash. The second
// return value tells us whether or not the rune was
// preceeded by a backslash (escaped).
func readPossiblyEscapedRune(reader *strings.Reader) (rune, bool, error) {
	rn, _, err := reader.ReadRune()
	if err != nil {
		return 0, false, err
	}
	if rn == '\\' {
		// Discard the backslash and read the next rune.
		rn, _, err = reader.ReadRune()
		if err != nil {
			return 0, false, err
		}
		return rn, true, nil
	}
	return rn, false, nil
}

func decodeAclItemArray(vr *ValueReader) []AclItem {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into []AclItem"))
		return nil
	}

	str := vr.ReadString(vr.Len())

	// Short-circuit empty array.
	if str == "{}" {
		return []AclItem{}
	}

	// Remove the '{' at the front and the '}' at the end,
	// so that parseAclItemArray doesn't have to deal with them.
	str = str[1 : len(str)-1]
	aclItems, err := parseAclItemArray(str)
	if err != nil {
		vr.Fatal(ProtocolError(err.Error()))
		return nil
	}
	return aclItems
}
