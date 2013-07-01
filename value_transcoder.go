package pgx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

type valueTranscoder struct {
	DecodeText   func(*MessageReader, int32) interface{}
	DecodeBinary func(*MessageReader, int32) interface{}
	EncodeTo     func(*bytes.Buffer, interface{})
	EncodeFormat int16
}

var valueTranscoders map[oid]*valueTranscoder
var defaultTranscoder *valueTranscoder

func init() {
	valueTranscoders = make(map[oid]*valueTranscoder)

	// bool
	valueTranscoders[oid(16)] = &valueTranscoder{
		DecodeText: decodeBoolFromText,
		EncodeTo:   encodeBool}

	// int8
	valueTranscoders[oid(20)] = &valueTranscoder{
		DecodeText: decodeInt8FromText,
		EncodeTo:   encodeInt8}

	// int2
	valueTranscoders[oid(21)] = &valueTranscoder{
		DecodeText: decodeInt2FromText,
		EncodeTo:   encodeInt2}

	// int4
	valueTranscoders[oid(23)] = &valueTranscoder{
		DecodeText: decodeInt4FromText,
		EncodeTo:   encodeInt4}

	// text
	valueTranscoders[oid(25)] = &valueTranscoder{
		DecodeText: decodeTextFromText,
		EncodeTo:   encodeText}

	// float4
	valueTranscoders[oid(700)] = &valueTranscoder{
		DecodeText: decodeFloat4FromText,
		EncodeTo:   encodeFloat4}

	// float8
	valueTranscoders[oid(701)] = &valueTranscoder{
		DecodeText: decodeFloat8FromText,
		EncodeTo:   encodeFloat8}

	// varchar -- same as text
	valueTranscoders[oid(1043)] = valueTranscoders[oid(25)]

	// use text transcoder for anything we don't understand
	defaultTranscoder = valueTranscoders[oid(25)]
}

func decodeBoolFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	switch s {
	case "t":
		return true
	case "f":
		return false
	default:
		panic(fmt.Sprintf("Received invalid bool: %v", s))
	}
}

func encodeBool(buf *bytes.Buffer, value interface{}) {
	v := value.(bool)
	s := strconv.FormatBool(v)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}

func decodeInt8FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Received invalid int8: %v", s))
	}
	return n
}

func encodeInt8(buf *bytes.Buffer, value interface{}) {
	v := value.(int64)
	s := strconv.FormatInt(int64(v), 10)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}

func decodeInt2FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		panic(fmt.Sprintf("Received invalid int2: %v", s))
	}
	return int16(n)
}

func encodeInt2(buf *bytes.Buffer, value interface{}) {
	v := value.(int16)
	s := strconv.FormatInt(int64(v), 10)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}

func decodeInt4FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Received invalid int4: %v", s))
	}
	return int32(n)
}

func encodeInt4(buf *bytes.Buffer, value interface{}) {
	v := value.(int32)
	s := strconv.FormatInt(int64(v), 10)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}

func decodeFloat4FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseFloat(s, 32)
	if err != nil {
		panic(fmt.Sprintf("Received invalid float4: %v", s))
	}
	return float32(n)
}

func encodeFloat4(buf *bytes.Buffer, value interface{}) {
	v := value.(float32)
	s := strconv.FormatFloat(float64(v), 'e', -1, 32)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}

func decodeFloat8FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Sprintf("Received invalid float8: %v", s))
	}
	return v
}

func encodeFloat8(buf *bytes.Buffer, value interface{}) {
	v := value.(float64)
	s := strconv.FormatFloat(float64(v), 'e', -1, 64)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}

func decodeTextFromText(mr *MessageReader, size int32) interface{} {
	return mr.ReadByteString(size)
}

func encodeText(buf *bytes.Buffer, value interface{}) {
	s := value.(string)
	binary.Write(buf, binary.BigEndian, int32(len(s)))
	buf.WriteString(s)
}
