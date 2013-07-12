package pgx

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"unsafe"
)

type valueTranscoder struct {
	DecodeText   func(*MessageReader, int32) interface{}
	DecodeBinary func(*MessageReader, int32) interface{}
	EncodeTo     func(*messageWriter, interface{})
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

	// bytea
	valueTranscoders[oid(17)] = &valueTranscoder{
		DecodeText:   decodeByteaFromText,
		EncodeTo:     encodeBytea,
		EncodeFormat: 1}

	// int8
	valueTranscoders[oid(20)] = &valueTranscoder{
		DecodeText:   decodeInt8FromText,
		DecodeBinary: decodeInt8FromBinary,
		EncodeTo:     encodeInt8,
		EncodeFormat: 1}

	// int2
	valueTranscoders[oid(21)] = &valueTranscoder{
		DecodeText:   decodeInt2FromText,
		DecodeBinary: decodeInt2FromBinary,
		EncodeTo:     encodeInt2,
		EncodeFormat: 1}

	// int4
	valueTranscoders[oid(23)] = &valueTranscoder{
		DecodeText:   decodeInt4FromText,
		DecodeBinary: decodeInt4FromBinary,
		EncodeTo:     encodeInt4,
		EncodeFormat: 1}

	// text
	valueTranscoders[oid(25)] = &valueTranscoder{
		DecodeText: decodeTextFromText,
		EncodeTo:   encodeText}

	// float4
	valueTranscoders[oid(700)] = &valueTranscoder{
		DecodeText:   decodeFloat4FromText,
		DecodeBinary: decodeFloat4FromBinary,
		EncodeTo:     encodeFloat4,
		EncodeFormat: 1}

	// float8
	valueTranscoders[oid(701)] = &valueTranscoder{
		DecodeText:   decodeFloat8FromText,
		DecodeBinary: decodeFloat8FromBinary,
		EncodeTo:     encodeFloat8,
		EncodeFormat: 1}

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

func encodeBool(w *messageWriter, value interface{}) {
	v := value.(bool)
	s := strconv.FormatBool(v)
	w.write(int32(len(s)))
	w.writeString(s)
}

func decodeInt8FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Received invalid int8: %v", s))
	}
	return n
}

func decodeInt8FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 8 {
		panic("Received an invalid size for an int8")
	}
	return mr.ReadInt64()
}

func encodeInt8(w *messageWriter, value interface{}) {
	v := value.(int64)
	w.write(int32(8))
	w.write(v)
}

func decodeInt2FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		panic(fmt.Sprintf("Received invalid int2: %v", s))
	}
	return int16(n)
}

func decodeInt2FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 2 {
		panic("Received an invalid size for an int8")
	}
	return mr.ReadInt16()
}

func encodeInt2(w *messageWriter, value interface{}) {
	v := value.(int16)
	w.write(int32(2))
	w.write(v)
}

func decodeInt4FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Received invalid int4: %v", s))
	}
	return int32(n)
}

func decodeInt4FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 4 {
		panic("Received an invalid size for an int4")
	}
	return mr.ReadInt32()
}

func encodeInt4(w *messageWriter, value interface{}) {
	v := value.(int32)
	w.write(int32(4))
	w.write(v)
}

func decodeFloat4FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	n, err := strconv.ParseFloat(s, 32)
	if err != nil {
		panic(fmt.Sprintf("Received invalid float4: %v", s))
	}
	return float32(n)
}

func decodeFloat4FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 4 {
		panic("Received an invalid size for an float4")
	}

	i := mr.ReadInt32()
	p := unsafe.Pointer(&i)
	return *(*float32)(p)
}

func encodeFloat4(w *messageWriter, value interface{}) {
	v := value.(float32)
	w.write(int32(4))
	w.write(v)
}

func decodeFloat8FromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Sprintf("Received invalid float8: %v", s))
	}
	return v
}

func decodeFloat8FromBinary(mr *MessageReader, size int32) interface{} {
	if size != 8 {
		panic("Received an invalid size for an float4")
	}

	i := mr.ReadInt64()
	p := unsafe.Pointer(&i)
	return *(*float64)(p)
}

func encodeFloat8(w *messageWriter, value interface{}) {
	v := value.(float64)
	w.write(int32(8))
	w.write(v)
}

func decodeTextFromText(mr *MessageReader, size int32) interface{} {
	return mr.ReadByteString(size)
}

func encodeText(w *messageWriter, value interface{}) {
	s := value.(string)
	w.write(int32(len(s)))
	w.writeString(s)
}

func decodeByteaFromText(mr *MessageReader, size int32) interface{} {
	s := mr.ReadByteString(size)
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		panic("Can't decode byte array")
	}
	return b
}

func encodeBytea(w *messageWriter, value interface{}) {
	b := value.([]byte)
	w.write(int32(len(b)))
	w.write(b)
}
