package pgx

import (
	"bytes"
	"encoding/binary"
)

type MessageReader []byte

func newMessageReader(buf *bytes.Buffer) *MessageReader {
	r := MessageReader(buf.Bytes())
	return &r
}

func (r *MessageReader) ReadByte() byte {
	b := (*r)[0]
	*r = (*r)[1:]
	return b
}

func (r *MessageReader) ReadInt16() int16 {
	n := int16(binary.BigEndian.Uint16((*r)[:2]))
	*r = (*r)[2:]
	return n
}

func (r *MessageReader) ReadInt32() int32 {
	n := int32(binary.BigEndian.Uint32((*r)[:4]))
	*r = (*r)[4:]
	return n
}

func (r *MessageReader) ReadInt64() int64 {
	n := int64(binary.BigEndian.Uint64((*r)[:8]))
	*r = (*r)[8:]
	return n
}

func (r *MessageReader) ReadOid() oid {
	n := oid(binary.BigEndian.Uint32((*r)[:4]))
	*r = (*r)[4:]
	return n
}

func (r *MessageReader) ReadString() string {
	n := bytes.IndexByte(*r, 0)
	s := (*r)[:n]
	*r = (*r)[n+1:]
	return string(s)
}

// Read count bytes and return as string
func (r *MessageReader) ReadByteString(count int32) string {
	s := (*r)[:count]
	*r = (*r)[count:]
	return string(s)
}
