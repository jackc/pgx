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

type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

type Scanner interface {
	Scan(qr *QueryResult, fd *FieldDescription, size int32) error
}

// TextEncoder is an interface used to encode values in text format for
// transmission to the PostgreSQL server. It is used by unprepared
// queries and for prepared queries when the type does not implement
// BinaryEncoder
type TextEncoder interface {
	// EncodeText MUST sanitize (and quote, if necessary) the returned string.
	// It will be interpolated directly into the SQL string.
	EncodeText() (string, error)
}

// BinaryEncoder is an interface used to encode values in binary format for
// transmission to the PostgreSQL server. It is used by prepared queries.
type BinaryEncoder interface {
	// EncodeText MUST sanitize (and quote, if necessary) the returned string.
	// It will be interpolated directly into the SQL string.
	EncodeBinary(w *WriteBuf) error
}

type NullInt64 struct {
	Int64 int64
	Valid bool // Valid is true if Int64 is not NULL
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

		switch arg := args[n-1].(type) {
		case string:
			return QuoteString(arg)
		case int:
			return strconv.FormatInt(int64(arg), 10)
		case int8:
			return strconv.FormatInt(int64(arg), 10)
		case int16:
			return strconv.FormatInt(int64(arg), 10)
		case int32:
			return strconv.FormatInt(int64(arg), 10)
		case int64:
			return strconv.FormatInt(int64(arg), 10)
		case time.Time:
			return QuoteString(arg.Format("2006-01-02 15:04:05.999999 -0700"))
		case uint:
			return strconv.FormatUint(uint64(arg), 10)
		case uint8:
			return strconv.FormatUint(uint64(arg), 10)
		case uint16:
			return strconv.FormatUint(uint64(arg), 10)
		case uint32:
			return strconv.FormatUint(uint64(arg), 10)
		case uint64:
			return strconv.FormatUint(uint64(arg), 10)
		case float32:
			return strconv.FormatFloat(float64(arg), 'f', -1, 32)
		case float64:
			return strconv.FormatFloat(arg, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(arg)
		case []byte:
			return `E'\\x` + hex.EncodeToString(arg) + `'`
		case nil:
			return "null"
		case TextEncoder:
			var s string
			s, err = arg.EncodeText()
			return s
		default:
			err = SerializationError(fmt.Sprintf("%T is not a core type and it does not implement TextEncoder", arg))
			return ""
		}
	}

	output = literalPattern.ReplaceAllStringFunc(sql, replacer)
	return
}

func (n *NullInt64) Scan(qr *QueryResult, fd *FieldDescription, size int32) error {
	if size == -1 {
		n.Int64, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Int64 = decodeInt8(qr, fd, size)
	return qr.Err()
}

func (n *NullInt64) EncodeText() (string, error) {
	if n.Valid {
		return strconv.FormatInt(int64(n.Int64), 10), nil
	} else {
		return "null", nil
	}
}

func decodeBool(qr *QueryResult, fd *FieldDescription, size int32) bool {
	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		switch s {
		case "t":
			return true
		case "f":
			return false
		default:
			qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid bool: %v", s)))
			return false
		}
	case BinaryFormatCode:
		if size != 1 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an bool: %d", size)))
			return false
		}
		b := qr.mr.ReadByte()
		return b != 0
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeInt8(qr *QueryResult, fd *FieldDescription, size int32) int64 {
	if fd.DataType != Int8Oid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read %v but received: %v", Int8Oid, fd.DataType)))
		return 0
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid int8: %v", s)))
			return 0
		}
		return n
	case BinaryFormatCode:
		if size != 8 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int8: %d", size)))
			return 0
		}
		return qr.mr.ReadInt64()
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeInt2(qr *QueryResult, fd *FieldDescription, size int32) int16 {
	if fd.DataType != Int2Oid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read %v but received: %v", Int2Oid, fd.DataType)))
		return 0
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		n, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid int2: %v", s)))
			return 0
		}
		return int16(n)
	case BinaryFormatCode:
		if size != 2 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int2: %d", size)))
			return 0
		}
		return qr.mr.ReadInt16()
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeInt4(qr *QueryResult, fd *FieldDescription, size int32) int32 {
	if fd.DataType != Int4Oid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read %v but received: %v", Int4Oid, fd.DataType)))
		return 0
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid int4: %v", s)))
		}
		return int32(n)
	case BinaryFormatCode:
		if size != 4 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an int4: %d", size)))
			return 0
		}
		return qr.mr.ReadInt32()
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeFloat4(qr *QueryResult, fd *FieldDescription, size int32) float32 {
	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		n, err := strconv.ParseFloat(s, 32)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid float4: %v", s)))
			return 0
		}
		return float32(n)
	case BinaryFormatCode:
		if size != 4 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float4: %d", size)))
			return 0
		}

		i := qr.mr.ReadInt32()
		p := unsafe.Pointer(&i)
		return *(*float32)(p)
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeFloat8(qr *QueryResult, fd *FieldDescription, size int32) float64 {
	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid float8: %v", s)))
			return 0
		}
		return v
	case BinaryFormatCode:
		if size != 8 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an float8: %d", size)))
			return 0
		}

		i := qr.mr.ReadInt64()
		p := unsafe.Pointer(&i)
		return *(*float64)(p)
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeText(qr *QueryResult, fd *FieldDescription, size int32) string {
	return qr.mr.ReadString(size)
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

func decodeBytea(qr *QueryResult, fd *FieldDescription, size int32) []byte {
	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		b, err := hex.DecodeString(s[2:])
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Can't decode byte array: %v - %v", err, s)))
			return nil
		}
		return b
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeDate(qr *QueryResult, fd *FieldDescription, size int32) time.Time {
	var zeroTime time.Time

	if fd.DataType != DateOid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read date but received: %v", fd.DataType)))
		return zeroTime
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		t, err := time.ParseInLocation("2006-01-02", s, time.Local)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Can't decode date: %v", s)))
			return zeroTime
		}
		return t
	case BinaryFormatCode:
		if size != 4 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an date: %d", size)))
		}
		dayOffset := qr.mr.ReadInt32()
		return time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.Local)
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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

func decodeTimestampTz(qr *QueryResult, fd *FieldDescription, size int32) time.Time {
	var zeroTime time.Time

	if fd.DataType != TimestampTzOid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read timestamptz but received: %v", fd.DataType)))
		return zeroTime
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)
		t, err := time.Parse("2006-01-02 15:04:05.999999-07", s)
		if err != nil {
			qr.Fatal(ProtocolError(fmt.Sprintf("Can't decode timestamptz: %v - %v", err, s)))
			return zeroTime
		}
		return t
	case BinaryFormatCode:
		if size != 8 {
			qr.Fatal(ProtocolError(fmt.Sprintf("Received an invalid size for an timestamptz: %d", size)))
		}
		microsecFromUnixEpochToY2K := int64(946684800 * 1000000)
		microsecSinceY2K := qr.mr.ReadInt64()
		microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
		return time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
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
