package pgx

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
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
	Int2ArrayOid   = 1005
	Int4ArrayOid   = 1007
	Int8ArrayOid   = 1016
	VarcharOid     = 1043
	DateOid        = 1082
	TimestampTzOid = 1184
)

const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

// ValueTranscoder stores all the data necessary to encode and decode values from
// a PostgreSQL server
type ValueTranscoder struct {
	// Decode decodes values returned from the server
	Decode func(qr *QueryResult, fd *FieldDescription, size int32) interface{}
	// DecodeFormat is the preferred response format.
	// Allowed values: TextFormatCode, BinaryFormatCode
	DecodeFormat int16
	// EncodeTo encodes values to send to the server
	EncodeTo func(*WriteBuf, interface{}) error
	// EncodeFormat is the format values are encoded for transmission.
	// Allowed values: TextFormatCode, BinaryFormatCode
	EncodeFormat int16
}

// ValueTranscoders is used to transcode values being sent to and received from
// the PostgreSQL server. Additional types can be transcoded by adding a
// *ValueTranscoder for the appropriate Oid to the map.
var ValueTranscoders map[Oid]*ValueTranscoder

var defaultTranscoder *ValueTranscoder

func init() {
	ValueTranscoders = make(map[Oid]*ValueTranscoder)

	// bool
	ValueTranscoders[BoolOid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeBool(qr, fd, size) },
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeBool,
		EncodeFormat: BinaryFormatCode}

	// bytea
	ValueTranscoders[ByteaOid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeBytea(qr, fd, size) },
		DecodeFormat: TextFormatCode,
		EncodeTo:     encodeBytea,
		EncodeFormat: BinaryFormatCode}

	// int8
	ValueTranscoders[Int8Oid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeInt8(qr, fd, size) },
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeInt8,
		EncodeFormat: BinaryFormatCode}

	// int2
	ValueTranscoders[Int2Oid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeInt2(qr, fd, size) },
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeInt2,
		EncodeFormat: BinaryFormatCode}

	// int4
	ValueTranscoders[Int4Oid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeInt4(qr, fd, size) },
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeInt4,
		EncodeFormat: BinaryFormatCode}

	// text
	ValueTranscoders[TextOid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeText(qr, fd, size) },
		DecodeFormat: TextFormatCode,
		EncodeTo:     encodeText,
		EncodeFormat: TextFormatCode}

	// float4
	ValueTranscoders[Float4Oid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeFloat4(qr, fd, size) },
		EncodeTo:     encodeFloat4,
		EncodeFormat: BinaryFormatCode}

	// float8
	ValueTranscoders[Float8Oid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeFloat8(qr, fd, size) },
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeFloat8,
		EncodeFormat: BinaryFormatCode}

	// int2[]
	ValueTranscoders[Int2ArrayOid] = &ValueTranscoder{
		Decode: func(qr *QueryResult, fd *FieldDescription, size int32) interface{} {
			return decodeInt2Array(qr, fd, size)
		},
		DecodeFormat: TextFormatCode,
		EncodeTo:     encodeInt2Array,
		EncodeFormat: TextFormatCode}

	// int4[]
	ValueTranscoders[Int4ArrayOid] = &ValueTranscoder{
		Decode: func(qr *QueryResult, fd *FieldDescription, size int32) interface{} {
			return decodeInt4Array(qr, fd, size)
		},
		DecodeFormat: TextFormatCode,
		EncodeTo:     encodeInt4Array,
		EncodeFormat: TextFormatCode}

	// int8[]
	ValueTranscoders[Int8ArrayOid] = &ValueTranscoder{
		Decode: func(qr *QueryResult, fd *FieldDescription, size int32) interface{} {
			return decodeInt8Array(qr, fd, size)
		},
		DecodeFormat: TextFormatCode,
		EncodeTo:     encodeInt8Array,
		EncodeFormat: TextFormatCode}

	// varchar -- same as text
	ValueTranscoders[VarcharOid] = ValueTranscoders[Oid(25)]

	// date
	ValueTranscoders[DateOid] = &ValueTranscoder{
		Decode:       func(qr *QueryResult, fd *FieldDescription, size int32) interface{} { return decodeDate(qr, fd, size) },
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeDate,
		EncodeFormat: TextFormatCode}

	// timestamptz
	ValueTranscoders[TimestampTzOid] = &ValueTranscoder{
		Decode: func(qr *QueryResult, fd *FieldDescription, size int32) interface{} {
			return decodeTimestampTz(qr, fd, size)
		},
		DecodeFormat: BinaryFormatCode,
		EncodeTo:     encodeTimestampTz,
		EncodeFormat: TextFormatCode}

	// use text transcoder for anything we don't understand
	defaultTranscoder = ValueTranscoders[TextOid]
}

var arrayEl *regexp.Regexp = regexp.MustCompile(`[{,](?:"((?:[^"\\]|\\.)*)"|(NULL)|([^,}]+))`)

// SplitArrayText is used by array transcoders to split array text into elements
func SplitArrayText(text string) (elements []string) {
	matches := arrayEl.FindAllStringSubmatch(text, -1)
	elements = make([]string, 0, len(matches))
	for _, match := range matches {
		if match[1] != "" {
			elements = append(elements, match[1])
		} else if match[2] != "" {
			elements = append(elements, match[2])
		} else if match[3] != "" {
			elements = append(elements, match[3])
		}
	}
	return
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

func decodeInt2Array(qr *QueryResult, fd *FieldDescription, size int32) []int16 {
	if fd.DataType != Int2ArrayOid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read int2[] but received: %v", fd.DataType)))
		return nil
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)

		elements := SplitArrayText(s)

		numbers := make([]int16, 0, len(elements))

		for _, e := range elements {
			n, err := strconv.ParseInt(e, 10, 16)
			if err != nil {
				qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid int2[]: %v", s)))
				return nil
			}
			numbers = append(numbers, int16(n))
		}

		return numbers
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return nil
	}
}

func int16SliceToArrayString(nums []int16) (string, error) {
	w := &bytes.Buffer{}
	_, err := w.WriteString("{")
	if err != nil {
		return "", err
	}

	for i, n := range nums {
		if i > 0 {
			_, err = w.WriteString(",")
			if err != nil {
				return "", err
			}
		}

		_, err = w.WriteString(strconv.FormatInt(int64(n), 10))
		if err != nil {
			return "", err
		}
	}

	_, err = w.WriteString("}")
	if err != nil {
		return "", err
	}

	return w.String(), nil
}

func encodeInt2Array(w *WriteBuf, value interface{}) error {
	v, ok := value.([]int16)
	if !ok {
		return fmt.Errorf("Expected []int16, received %T", value)
	}

	s, err := int16SliceToArrayString(v)
	if err != nil {
		return fmt.Errorf("Failed to encode []int16: %v", err)
	}

	return encodeText(w, s)
}

func decodeInt4Array(qr *QueryResult, fd *FieldDescription, size int32) []int32 {
	if fd.DataType != Int4ArrayOid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read int4[] but received: %v", fd.DataType)))
		return nil
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)

		elements := SplitArrayText(s)

		numbers := make([]int32, 0, len(elements))

		for _, e := range elements {
			n, err := strconv.ParseInt(e, 10, 32)
			if err != nil {
				qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid int4[]: %v", s)))
				return nil
			}
			numbers = append(numbers, int32(n))
		}

		return numbers
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return nil
	}
}

func int32SliceToArrayString(nums []int32) (string, error) {
	w := &bytes.Buffer{}

	_, err := w.WriteString("{")
	if err != nil {
		return "", err
	}

	for i, n := range nums {
		if i > 0 {
			_, err = w.WriteString(",")
			if err != nil {
				return "", err
			}
		}

		_, err = w.WriteString(strconv.FormatInt(int64(n), 10))
		if err != nil {
			return "", err
		}
	}

	_, err = w.WriteString("}")
	if err != nil {
		return "", err
	}

	return w.String(), nil
}

func encodeInt4Array(w *WriteBuf, value interface{}) error {
	v, ok := value.([]int32)
	if !ok {
		return fmt.Errorf("Expected []int32, received %T", value)
	}

	s, err := int32SliceToArrayString(v)
	if err != nil {
		return fmt.Errorf("Failed to encode []int32: %v", err)
	}

	return encodeText(w, s)
}

func decodeInt8Array(qr *QueryResult, fd *FieldDescription, size int32) []int64 {
	if fd.DataType != Int8ArrayOid {
		qr.Fatal(ProtocolError(fmt.Sprintf("Tried to read int8[] but received: %v", fd.DataType)))
		return nil
	}

	switch fd.FormatCode {
	case TextFormatCode:
		s := qr.mr.ReadString(size)

		elements := SplitArrayText(s)

		numbers := make([]int64, 0, len(elements))

		for _, e := range elements {
			n, err := strconv.ParseInt(e, 10, 64)
			if err != nil {
				qr.Fatal(ProtocolError(fmt.Sprintf("Received invalid int8[]: %v", s)))
				return nil
			}
			numbers = append(numbers, int64(n))
		}

		return numbers
	default:
		qr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fd.FormatCode)))
		return nil
	}
}

func int64SliceToArrayString(nums []int64) (string, error) {
	w := &bytes.Buffer{}

	_, err := w.WriteString("{")
	if err != nil {
		return "", err
	}

	for i, n := range nums {
		if i > 0 {
			_, err = w.WriteString(",")
			if err != nil {
				return "", err
			}
		}

		_, err = w.WriteString(strconv.FormatInt(int64(n), 10))
		if err != nil {
			return "", err
		}
	}

	_, err = w.WriteString("}")
	if err != nil {
		return "", err
	}

	return w.String(), nil
}

func encodeInt8Array(w *WriteBuf, value interface{}) error {
	v, ok := value.([]int64)
	if !ok {
		return fmt.Errorf("Expected []int64, received %T", value)
	}

	s, err := int64SliceToArrayString(v)
	if err != nil {
		return fmt.Errorf("Failed to encode []int64: %v", err)
	}

	return encodeText(w, s)
}
