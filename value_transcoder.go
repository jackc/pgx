package pgx

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"
	"unsafe"
)

// ValueTranscoder stores all the data necessary to encode and decode values from
// a PostgreSQL server
type ValueTranscoder struct {
	// DecodeText decodes values returned from the server in text format
	DecodeText func(*MessageReader, int32) interface{}
	// DecodeBinary decodes values returned from the server in binary format
	DecodeBinary func(*MessageReader, int32) interface{}
	// EncodeTo encodes values to send to the server
	EncodeTo func(io.Writer, interface{}) error
	// EncodeFormat is the format values are encoded for transmission.
	// 0 = text
	// 1 = binary
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
	ValueTranscoders[Oid(16)] = &ValueTranscoder{
		DecodeText:   decodeBoolFromText,
		DecodeBinary: decodeBoolFromBinary,
		EncodeTo:     encodeBool,
		EncodeFormat: 1}

	// bytea
	ValueTranscoders[Oid(17)] = &ValueTranscoder{
		DecodeText:   decodeByteaFromText,
		EncodeTo:     encodeBytea,
		EncodeFormat: 1}

	// int8
	ValueTranscoders[Oid(20)] = &ValueTranscoder{
		DecodeText:   decodeInt8FromText,
		DecodeBinary: decodeInt8FromBinary,
		EncodeTo:     encodeInt8,
		EncodeFormat: 1}

	// int2
	ValueTranscoders[Oid(21)] = &ValueTranscoder{
		DecodeText:   decodeInt2FromText,
		DecodeBinary: decodeInt2FromBinary,
		EncodeTo:     encodeInt2,
		EncodeFormat: 1}

	// int4
	ValueTranscoders[Oid(23)] = &ValueTranscoder{
		DecodeText:   decodeInt4FromText,
		DecodeBinary: decodeInt4FromBinary,
		EncodeTo:     encodeInt4,
		EncodeFormat: 1}

	// text
	ValueTranscoders[Oid(25)] = &ValueTranscoder{
		DecodeText: decodeTextFromText,
		EncodeTo:   encodeText}

	// float4
	ValueTranscoders[Oid(700)] = &ValueTranscoder{
		DecodeText:   decodeFloat4FromText,
		DecodeBinary: decodeFloat4FromBinary,
		EncodeTo:     encodeFloat4,
		EncodeFormat: 1}

	// float8
	ValueTranscoders[Oid(701)] = &ValueTranscoder{
		DecodeText:   decodeFloat8FromText,
		DecodeBinary: decodeFloat8FromBinary,
		EncodeTo:     encodeFloat8,
		EncodeFormat: 1}

	// int2[]
	ValueTranscoders[Oid(1005)] = &ValueTranscoder{
		DecodeText: decodeInt2ArrayFromText,
		EncodeTo:   encodeInt2Array}

	// int4[]
	ValueTranscoders[Oid(1007)] = &ValueTranscoder{
		DecodeText: decodeInt4ArrayFromText,
		EncodeTo:   encodeInt4Array}

	// int8[]
	ValueTranscoders[Oid(1016)] = &ValueTranscoder{
		DecodeText: decodeInt8ArrayFromText,
		EncodeTo:   encodeInt8Array}

	// varchar -- same as text
	ValueTranscoders[Oid(1043)] = ValueTranscoders[Oid(25)]

	// date
	ValueTranscoders[Oid(1082)] = &ValueTranscoder{
		DecodeText: decodeDateFromText,
		EncodeTo:   encodeDate}

	// timestamptz
	ValueTranscoders[Oid(1184)] = &ValueTranscoder{
		DecodeText:   decodeTimestampTzFromText,
		DecodeBinary: decodeTimestampTzFromBinary,
		EncodeTo:     encodeTimestampTz}

	// use text transcoder for anything we don't understand
	defaultTranscoder = ValueTranscoders[Oid(25)]
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

func decodeBoolFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	switch s {
	case "t":
		return true
	case "f":
		return false
	default:
		return ProtocolError(fmt.Sprintf("Received invalid bool: %v", s))
	}
}

func decodeBoolFromBinary(mr *MessageReader, size int32) interface{} {
	if size != 1 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an bool: %d", size))
	}
	b := mr.ReadByte()
	return b != 0
}

func encodeBool(w io.Writer, value interface{}) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("Expected bool, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(1))
	if err != nil {
		return err
	}

	var n byte
	if v {
		n = 1
	}

	return binary.Write(w, binary.BigEndian, n)
}

func decodeInt8FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Received invalid int8: %v", s))
	}
	return n
}

func decodeInt8FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 8 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an int8: %d", size))
	}
	return mr.ReadInt64()
}

func encodeInt8(w io.Writer, value interface{}) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("Expected int64, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(8))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, v)
}

func decodeInt2FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	n, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Received invalid int2: %v", s))
	}
	return int16(n)
}

func decodeInt2FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 2 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an int2: %d", size))
	}
	return mr.ReadInt16()
}

func encodeInt2(w io.Writer, value interface{}) error {
	v, ok := value.(int16)
	if !ok {
		return fmt.Errorf("Expected int16, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(2))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, v)
}

func decodeInt4FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Received invalid int4: %v", s))
	}
	return int32(n)
}

func decodeInt4FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 4 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an int4: %d", size))
	}
	return mr.ReadInt32()
}

func encodeInt4(w io.Writer, value interface{}) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("Expected int32, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(4))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, v)
}

func decodeFloat4FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	n, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Received invalid float4: %v", s))
	}
	return float32(n)
}

func decodeFloat4FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 4 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an float4: %d", size))
	}

	i := mr.ReadInt32()
	p := unsafe.Pointer(&i)
	return *(*float32)(p)
}

func encodeFloat4(w io.Writer, value interface{}) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("Expected float32, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(4))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, v)
}

func decodeFloat8FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Received invalid float8: %v", s))
	}
	return v
}

func decodeFloat8FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 8 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an float8: %d", size))
	}

	i := mr.ReadInt64()
	p := unsafe.Pointer(&i)
	return *(*float64)(p)
}

func encodeFloat8(w io.Writer, value interface{}) error {
	v, ok := value.(float64)
	if !ok {
		return fmt.Errorf("Expected float64, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(8))
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, v)
}

func decodeTextFromText(mr *MessageReader, size int32) interface{} {
	return mr.ReadString(size)
}

func encodeText(w io.Writer, value interface{}) error {
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("Expected string, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}

func decodeByteaFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return ProtocolError(fmt.Sprintf("Can't decode byte array: %v - %v", err, s))
	}
	return b
}

func encodeBytea(w io.Writer, value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("Expected []byte, received %T", value)
	}

	err := binary.Write(w, binary.BigEndian, int32(len(b)))
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	return err
}

func decodeDateFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	t, err := time.ParseInLocation("2006-01-02", s, time.Local)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Can't decode date: %v", s))
	}
	return t
}

func encodeDate(w io.Writer, value interface{}) error {
	t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("Expected time.Time, received %T", value)
	}

	s := t.Format("2006-01-02")
	err := binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}

func decodeTimestampTzFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)
	t, err := time.Parse("2006-01-02 15:04:05.999999-07", s)
	if err != nil {
		return ProtocolError(fmt.Sprintf("Can't decode timestamptz: %v - %v", err, s))
	}
	return t
}

func decodeTimestampTzFromBinary(mr *MessageReader, size int32) interface{} {
	if size != 8 {
		return ProtocolError(fmt.Sprintf("Received an invalid size for an int8: %d", size))
	}
	microsecFromUnixEpochToY2K := int64(946684800 * 1000000)
	microsecSinceY2K := mr.ReadInt64()
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)

	// 2000-01-01 00:00:00 in 946684800
	// 946684800 * 1000000

}

func encodeTimestampTz(w io.Writer, value interface{}) error {
	t, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("Expected time.Time, received %T", value)
	}

	s := t.Format("2006-01-02 15:04:05.999999 -0700")
	err := binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}

func decodeInt2ArrayFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)

	elements := SplitArrayText(s)

	numbers := make([]int16, 0, len(elements))

	for _, e := range elements {
		n, err := strconv.ParseInt(e, 10, 16)
		if err != nil {
			return ProtocolError(fmt.Sprintf("Received invalid int2[]: %v", s))
		}
		numbers = append(numbers, int16(n))
	}

	return numbers
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

func encodeInt2Array(w io.Writer, value interface{}) error {
	v, ok := value.([]int16)
	if !ok {
		return fmt.Errorf("Expected []int16, received %T", value)
	}

	s, err := int16SliceToArrayString(v)
	if err != nil {
		return fmt.Errorf("Failed to encode []int16: %v", err)
	}

	err = binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}

func decodeInt4ArrayFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)

	elements := SplitArrayText(s)

	numbers := make([]int32, 0, len(elements))

	for _, e := range elements {
		n, err := strconv.ParseInt(e, 10, 16)
		if err != nil {
			return ProtocolError(fmt.Sprintf("Received invalid int4[]: %v", s))
		}
		numbers = append(numbers, int32(n))
	}

	return numbers
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

func encodeInt4Array(w io.Writer, value interface{}) error {
	v, ok := value.([]int32)
	if !ok {
		return fmt.Errorf("Expected []int32, received %T", value)
	}

	s, err := int32SliceToArrayString(v)
	if err != nil {
		return fmt.Errorf("Failed to encode []int32: %v", err)
	}

	err = binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}

func decodeInt8ArrayFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadString(size)

	elements := SplitArrayText(s)

	numbers := make([]int64, 0, len(elements))

	for _, e := range elements {
		n, err := strconv.ParseInt(e, 10, 16)
		if err != nil {
			return ProtocolError(fmt.Sprintf("Received invalid int8[]: %v", s))
		}
		numbers = append(numbers, int64(n))
	}

	return numbers
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

func encodeInt8Array(w io.Writer, value interface{}) error {
	v, ok := value.([]int64)
	if !ok {
		return fmt.Errorf("Expected []int64, received %T", value)
	}

	s, err := int64SliceToArrayString(v)
	if err != nil {
		return fmt.Errorf("Failed to encode []int64: %v", err)
	}

	err = binary.Write(w, binary.BigEndian, int32(len(s)))
	if err != nil {
		return err
	}

	_, err = io.WriteString(w, s)
	return err
}
