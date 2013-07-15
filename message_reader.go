package pgx

import (
	"bytes"
	"encoding/binary"
)

// MessageReader is a helper that reads values from a PostgreSQL message.
type MessageReader struct {
	buf *bytes.Buffer
}

func newMessageReader(buf *bytes.Buffer) *MessageReader {
	return &MessageReader{buf: buf}
}

func (r *MessageReader) ReadByte() byte {
	b, err := r.buf.ReadByte()
	if err != nil {
		panic("Unable to read byte")
	}
	return b
}

func (r *MessageReader) ReadInt16() int16 {
	return int16(binary.BigEndian.Uint16(r.buf.Next(2)))
}

func (r *MessageReader) ReadInt32() int32 {
	return int32(binary.BigEndian.Uint32(r.buf.Next(4)))
}

func (r *MessageReader) ReadInt64() int64 {
	return int64(binary.BigEndian.Uint64(r.buf.Next(8)))
}

func (r *MessageReader) ReadOid() Oid {
	return Oid(binary.BigEndian.Uint32(r.buf.Next(4)))
}

// ReadString reads a null terminated string
func (r *MessageReader) ReadString() string {
	b, err := r.buf.ReadBytes(0)
	if err != nil {
		panic("Unable to read string")
	}
	return string(b[:len(b)-1])
}

// ReadByteString reads count bytes and return as string
func (r *MessageReader) ReadByteString(count int32) string {
	return string(r.buf.Next(int(count)))
}
