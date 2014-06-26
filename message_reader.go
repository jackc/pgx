package pgx

import (
	"bytes"
	"encoding/binary"
)

// MessageReader is a helper that reads values from a PostgreSQL message.
type MessageReader bytes.Buffer

func (r *MessageReader) ReadByte() (b byte) {
	b, _ = (*bytes.Buffer)(r).ReadByte()
	return
}

func (r *MessageReader) ReadInt16() (n int16) {
	b := (*bytes.Buffer)(r).Next(2)
	return int16(binary.BigEndian.Uint16(b))
}

func (r *MessageReader) ReadInt32() (n int32) {
	b := (*bytes.Buffer)(r).Next(4)
	return int32(binary.BigEndian.Uint32(b))
}

func (r *MessageReader) ReadInt64() (n int64) {
	b := (*bytes.Buffer)(r).Next(8)
	return int64(binary.BigEndian.Uint64(b))
}

func (r *MessageReader) ReadOid() (oid Oid) {
	b := (*bytes.Buffer)(r).Next(4)
	return Oid(binary.BigEndian.Uint32(b))
}

// ReadString reads a null terminated string
func (r *MessageReader) ReadCString() (s string) {
	b, _ := (*bytes.Buffer)(r).ReadBytes(0)
	return string(b[:len(b)-1])
}

// ReadString reads count bytes and returns as string
func (r *MessageReader) ReadString(count int32) (s string) {
	size := int(count)
	b := (*bytes.Buffer)(r).Next(size)
	return string(b)
}
