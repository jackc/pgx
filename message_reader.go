package pqx

import (
	"bytes"
	"encoding/binary"
)

type messageReader []byte

func newMessageReader(buf []byte) *messageReader {
	r := messageReader(buf)
	return &r
}

func (r *messageReader) readByte() byte {
	b := (*r)[0]
	*r = (*r)[1:]
	return b
}

func (r *messageReader) readInt16() int16 {
	n := int16(binary.BigEndian.Uint16((*r)[:2]))
	*r = (*r)[2:]
	return n
}

func (r *messageReader) readInt32() int32 {
	n := int32(binary.BigEndian.Uint32((*r)[:4]))
	*r = (*r)[4:]
	return n
}

func (r *messageReader) readOid() oid {
	n := oid(binary.BigEndian.Uint32((*r)[:4]))
	*r = (*r)[4:]
	return n
}

func (r *messageReader) readString() string {
	n := bytes.IndexByte(*r, 0)
	s := (*r)[:n]
	*r = (*r)[n+1:]
	return string(s)
}

// Read count bytes and return as string
func (r *messageReader) readByteString(count int32) string {
	s := (*r)[:count]
	*r = (*r)[count:]
	return string(s)
}
