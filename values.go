package pgx

import (
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	BoolOid        = 16
	ByteaOid       = 17
	Int8Oid        = 20
	Int2Oid        = 21
	Int4Oid        = 23
	TextOid        = 25
	Float4Oid      = 700
	Float8Oid      = 701
	VarcharOid     = 1043
	DateOid        = 1082
	TimestampTzOid = 1184
)

const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

const (
	NullText   = iota
	SafeText   = iota
	UnsafeText = iota
)

type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

// Scanner is an interface used to decode values from the PostgreSQL server.
type Scanner interface {
	// Scan MUST check fd's DataType and FormatCode before decoding. It should
	// not assume that it was called on the type of value.
	Scan(rows *Rows, fd *FieldDescription, size int32) error
}

// TextEncoder is an interface used to encode values in text format for
// transmission to the PostgreSQL server. It is used by unprepared
// queries and for prepared queries when the type does not implement
// BinaryEncoder
type TextEncoder interface {
	// EncodeText returns the value encoded into a string. status must be
	// NullText if the value is NULL, UnsafeText if the value should be quoted
	// and escaped, or SafeText if the value should not be quoted.
	EncodeText() (val string, status byte, err error)
}

// BinaryEncoder is an interface used to encode values in binary format for
// transmission to the PostgreSQL server. It is used by prepared queries.
type BinaryEncoder interface {
	// EncodeBinary writes the binary value to w
	EncodeBinary(w *WriteBuf) error
}

// NullFloat32 represents an smallint that may be null.
// NullFloat32 implements the Scanner, TextEncoder, and BinaryEncoder interfaces
// so it may be used both as an argument to Query[Row] and a destination for
// Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullFloat32 struct {
	Float32 float32
	Valid   bool // Valid is true if Float32 is not NULL
}

func (n *NullFloat32) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Float32, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Float32 = decodeFloat4(rows, fd, size)
	return rows.Err()
}

func (n NullFloat32) EncodeText() (string, byte, error) {
	if n.Valid {
		return strconv.FormatFloat(float64(n.Float32), 'f', -1, 32), SafeText, nil
	} else {
		return "", NullText, nil
	}
}

func (n NullFloat32) EncodeBinary(w *WriteBuf) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeFloat4(w, n.Float32)
}

// NullFloat64 represents an smallint that may be null.
// NullFloat64 implements the Scanner, TextEncoder, and BinaryEncoder interfaces
// so it may be used both as an argument to Query[Row] and a destination for
// Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullFloat64 struct {
	Float64 float64
	Valid   bool // Valid is true if Float64 is not NULL
}

func (n *NullFloat64) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Float64, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Float64 = decodeFloat8(rows, fd, size)
	return rows.Err()
}

func (n NullFloat64) EncodeText() (string, byte, error) {
	if n.Valid {
		return strconv.FormatFloat(n.Float64, 'f', -1, 64), SafeText, nil
	} else {
		return "", NullText, nil
	}
}

func (n NullFloat64) EncodeBinary(w *WriteBuf) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeFloat8(w, n.Float64)
}

// NullString represents an integer that may be null. NullString implements
// the Scanner and TextEncoder interfaces so it may be used both as an
// argument to Query[Row] and a destination for Scan for prepared and
// unprepared queries.
//
// If Valid is false then the value is NULL.
type NullString struct {
	String string
	Valid  bool // Valid is true if Int64 is not NULL
}

func (s *NullString) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		s.String, s.Valid = "", false
		return nil
	}
	s.Valid = true
	s.String = decodeText(rows, fd, size)
	return rows.Err()
}

func (s NullString) EncodeText() (string, byte, error) {
	if s.Valid {
		return s.String, UnsafeText, nil
	} else {
		return "", NullText, nil
	}
}

// NullInt16 represents an smallint that may be null.
// NullInt16 implements the Scanner, TextEncoder, and BinaryEncoder interfaces
// so it may be used both as an argument to Query[Row] and a destination for
// Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullInt16 struct {
	Int16 int16
	Valid bool // Valid is true if Int16 is not NULL
}

func (n *NullInt16) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Int16, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int16 = decodeInt2(rows, fd, size)
	return rows.Err()
}

func (n NullInt16) EncodeText() (string, byte, error) {
	if n.Valid {
		return strconv.FormatInt(int64(n.Int16), 10), SafeText, nil
	} else {
		return "", NullText, nil
	}
}

func (n NullInt16) EncodeBinary(w *WriteBuf) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeInt2(w, n.Int16)
}

// NullInt32 represents an smallint that may be null.
// NullInt32 implements the Scanner, TextEncoder, and BinaryEncoder interfaces
// so it may be used both as an argument to Query[Row] and a destination for
// Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullInt32 struct {
	Int32 int32
	Valid bool // Valid is true if Int64 is not NULL
}

func (n *NullInt32) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Int32, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int32 = decodeInt4(rows, fd, size)
	return rows.Err()
}

func (n NullInt32) EncodeText() (string, byte, error) {
	if n.Valid {
		return strconv.FormatInt(int64(n.Int32), 10), SafeText, nil
	} else {
		return "", NullText, nil
	}
}

func (n NullInt32) EncodeBinary(w *WriteBuf) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeInt4(w, n.Int32)
}

// NullInt64 represents an bigint that may be null.
// NullInt64 implements the Scanner, TextEncoder, and BinaryEncoder interfaces
// so it may be used both as an argument to Query[Row] and a destination for
// Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullInt64 struct {
	Int64 int64
	Valid bool // Valid is true if Int64 is not NULL
}

func (n *NullInt64) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Int64, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int64 = decodeInt8(rows, fd, size)
	return rows.Err()
}

func (n NullInt64) EncodeText() (string, byte, error) {
	if n.Valid {
		return strconv.FormatInt(int64(n.Int64), 10), SafeText, nil
	} else {
		return "", NullText, nil
	}
}

func (n NullInt64) EncodeBinary(w *WriteBuf) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeInt8(w, n.Int64)
}

// NullBool represents an bigint that may be null.
// NullBool implements the Scanner, TextEncoder, and BinaryEncoder interfaces
// so it may be used both as an argument to Query[Row] and a destination for
// Scan for prepared and unprepared queries.
//
// If Valid is false then the value is NULL.
type NullBool struct {
	Bool  bool
	Valid bool // Valid is true if Bool is not NULL
}

func (n *NullBool) Scan(rows *Rows, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Bool, n.Valid = false, false
		return nil
	}
	n.Valid = true
	n.Bool = decodeBool(rows, fd, size)
	return rows.Err()
}

func (n NullBool) EncodeText() (string, byte, error) {
	if n.Valid {
		return strconv.FormatBool(n.Bool), SafeText, nil
	} else {
		return "", NullText, nil
	}
}

func (n NullBool) EncodeBinary(w *WriteBuf) error {
	if !n.Valid {
		w.WriteInt32(-1)
		return nil
	}

	return encodeBool(w, n.Bool)
}

var literalPattern *regexp.Regexp = regexp.MustCompile(`\$\d+`)

// QuoteString escapes and quotes a string making it safe for interpolation
// into an SQL string.
func QuoteString(input string) (output string) {
	output = "'" + strings.Replace(input, "'", "''", -1) + "'"
	return
}

// QuoteIdentifier escapes and quotes an identifier making it safe for
// interpolation into an SQL string
func QuoteIdentifier(input string) (output string) {
	output = `"` + strings.Replace(input, `"`, `""`, -1) + `"`
	return
}

// SanitizeSql substitutely args positionaly into sql. Placeholder values are
// $ prefixed integers like $1, $2, $3, etc. args are sanitized and quoted as
// appropriate.
func SanitizeSql(sql string, args ...interface{}) (output string, err error) {
	replacer := func(match string) (replacement string) {
		if err != nil {
			return ""
		}

		n, _ := strconv.ParseInt(match[1:], 10, 0)
		if int(n-1) >= len(args) {
			err = fmt.Errorf("Cannot interpolate %v, only %d arguments provided", match, len(args))
			return
		}

		var s string
		s, err = sanitizeArg(args[n-1])
		return s
	}

	output = literalPattern.ReplaceAllStringFunc(sql, replacer)
	return
}

func sanitizeArg(arg interface{}) (string, error) {
	switch arg := arg.(type) {
	case string:
		return QuoteString(arg), nil
	case int:
		return strconv.FormatInt(int64(arg), 10), nil
	case int8:
		return strconv.FormatInt(int64(arg), 10), nil
	case int16:
		return strconv.FormatInt(int64(arg), 10), nil
	case int32:
		return strconv.FormatInt(int64(arg), 10), nil
	case int64:
		return strconv.FormatInt(int64(arg), 10), nil
	case time.Time:
		return QuoteString(arg.Format("2006-01-02 15:04:05.999999 -0700")), nil
	case uint:
		return strconv.FormatUint(uint64(arg), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(arg), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(arg), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(arg), 10), nil
	case uint64:
		return strconv.FormatUint(uint64(arg), 10), nil
	case float32:
		return strconv.FormatFloat(float64(arg), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(arg, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(arg), nil
	case []byte:
		return `E'\\x` + hex.EncodeToString(arg) + `'`, nil
	case nil:
		return "null", nil
	case TextEncoder:
		s, status, err := arg.EncodeText()
		switch status {
		case NullText:
			return "null", err
		case UnsafeText:
			return QuoteString(s), err
		case SafeText:
			return s, err
		default:
			return "", SerializationError("Received invalid status from EncodeText")
		}
	default:
		return "", SerializationError(fmt.Sprintf("%T is not a core type and it does not implement TextEncoder", arg))
	}
}

func decodeBool(rows *Rows, fd *FieldDescription, size int32) bool {
	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		switch s {
		case "t":
			return true
		case "f":
			return false
		default:
			rows.Fatal(ProtocolError(fmt.Sprintf("Received invalid bool: %v", s)))
			return false
		}
	case BinaryFormatCode:
		if size != 1 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an bool: %d", size)))
			return false
		}
		b := rows.mr.ReadByte()
		return b != 0
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return false
	}
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

func decodeInt8(rows *Rows, fd *FieldDescription, size int32) int64 {
	if fd.DataType != Int8Oid {
		rows.Fatal(ProtocolError(fmt.Sprintf("Expected type oid %v but received type oid %v", Int8Oid, fd.DataType)))
		return 0
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received invalid int8: %v", s)))
			return 0
		}
		return n
	case BinaryFormatCode:
		if size != 8 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int8: %d", size)))
			return 0
		}
		return rows.mr.ReadInt64()
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return 0
	}
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
			return fmt.Errorf("uint64 %d is larger than max int64 %d", value, math.MaxInt64)
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

func decodeInt2(rows *Rows, fd *FieldDescription, size int32) int16 {
	if fd.DataType != Int2Oid {
		rows.Fatal(ProtocolError(fmt.Sprintf("Expected type oid %v but received type oid %v", Int2Oid, fd.DataType)))
		return 0
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		n, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received invalid int2: %v", s)))
			return 0
		}
		return int16(n)
	case BinaryFormatCode:
		if size != 2 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int2: %d", size)))
			return 0
		}
		return rows.mr.ReadInt16()
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return 0
	}
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

func decodeInt4(rows *Rows, fd *FieldDescription, size int32) int32 {
	if fd.DataType != Int4Oid {
		rows.Fatal(ProtocolError(fmt.Sprintf("Expected type oid %v but received type oid %v", Int4Oid, fd.DataType)))
		return 0
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received invalid int4: %v", s)))
		}
		return int32(n)
	case BinaryFormatCode:
		if size != 4 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int4: %d", size)))
			return 0
		}
		return rows.mr.ReadInt32()
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return 0
	}
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
			return fmt.Errorf("%T %d is larger than max int64 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	case int64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int64 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	case uint64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int64 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	case int:
		if value > math.MaxInt32 {
			return fmt.Errorf("%T %d is larger than max int64 %d", value, value, math.MaxInt32)
		}
		v = int32(value)
	default:
		return fmt.Errorf("Expected integer representable in int32, received %T %v", value, value)
	}

	w.WriteInt32(4)
	w.WriteInt32(v)

	return nil
}

func decodeFloat4(rows *Rows, fd *FieldDescription, size int32) float32 {
	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		n, err := strconv.ParseFloat(s, 32)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received invalid float4: %v", s)))
			return 0
		}
		return float32(n)
	case BinaryFormatCode:
		if size != 4 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float4: %d", size)))
			return 0
		}

		i := rows.mr.ReadInt32()
		p := unsafe.Pointer(&i)
		return *(*float32)(p)
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return 0
	}
}

func encodeFloat4(w *WriteBuf, value interface{}) error {
	var v float32
	switch value := value.(type) {
	case float32:
		v = float32(value)
	case float64:
		if value > math.MaxFloat32 {
			return fmt.Errorf("%T %f is larger than max float32 %f", value, math.MaxFloat32)
		}
		v = float32(value)
	default:
		return fmt.Errorf("Expected float representable in float32, received %T %v", value, value)
	}

	w.WriteInt32(4)

	p := unsafe.Pointer(&v)
	w.WriteInt32(*(*int32)(p))

	return nil
}

func decodeFloat8(rows *Rows, fd *FieldDescription, size int32) float64 {
	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received invalid float8: %v", s)))
			return 0
		}
		return v
	case BinaryFormatCode:
		if size != 8 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float8: %d", size)))
			return 0
		}

		i := rows.mr.ReadInt64()
		p := unsafe.Pointer(&i)
		return *(*float64)(p)
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return 0
	}
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

	p := unsafe.Pointer(&v)
	w.WriteInt64(*(*int64)(p))

	return nil
}

func decodeText(rows *Rows, fd *FieldDescription, size int32) string {
	return rows.mr.ReadString(size)
}

func encodeText(w *WriteBuf, value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("Expected string, received %T", value)
	}

	w.WriteInt32(int32(len(s)))
	w.WriteBytes([]byte(s))

	return nil
}

func decodeBytea(rows *Rows, fd *FieldDescription, size int32) []byte {
	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		b, err := hex.DecodeString(s[2:])
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Can't decode byte array: %v - %v", err, s)))
			return nil
		}
		return b
	case BinaryFormatCode:
		return rows.mr.ReadBytes(size)
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return nil
	}
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

func decodeDate(rows *Rows, fd *FieldDescription, size int32) time.Time {
	var zeroTime time.Time

	if fd.DataType != DateOid {
		rows.Fatal(ProtocolError(fmt.Sprintf("Expected type oid %v but received type oid %v", DateOid, fd.DataType)))
		return zeroTime
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		t, err := time.ParseInLocation("2006-01-02", s, time.Local)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Can't decode date: %v", s)))
			return zeroTime
		}
		return t
	case BinaryFormatCode:
		if size != 4 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an date: %d", size)))
		}
		dayOffset := rows.mr.ReadInt32()
		return time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.Local)
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return zeroTime
	}
}

func encodeDate(w *WriteBuf, value interface{}) error {
	t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("Expected time.Time, received %T", value)
	}

	s := t.Format("2006-01-02")
	return encodeText(w, s)
}

func decodeTimestampTz(rows *Rows, fd *FieldDescription, size int32) time.Time {
	var zeroTime time.Time

	if fd.DataType != TimestampTzOid {
		rows.Fatal(ProtocolError(fmt.Sprintf("Expected type oid %v but received type oid %v", TimestampTzOid, fd.DataType)))
		return zeroTime
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := rows.mr.ReadString(size)
		t, err := time.Parse("2006-01-02 15:04:05.999999-07", s)
		if err != nil {
			rows.Fatal(ProtocolError(fmt.Sprintf("Can't decode timestamptz: %v - %v", err, s)))
			return zeroTime
		}
		return t
	case BinaryFormatCode:
		if size != 8 {
			rows.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an timestamptz: %d", size)))
		}
		microsecFromUnixEpochToY2K := int64(946684800 * 1000000)
		microsecSinceY2K := rows.mr.ReadInt64()
		microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
		return time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
	default:
		rows.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return zeroTime
	}
}

func encodeTimestampTz(w *WriteBuf, value interface{}) error {
	t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("Expected time.Time, received %T", value)
	}

	s := t.Format("2006-01-02 15:04:05.999999 -0700")
	return encodeText(w, s)
}
