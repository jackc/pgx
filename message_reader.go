package pgx

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// MessageReader is a helper that reads values from a PostgreSQL message.
// To avoid verbose error handling it internally records errors and no-ops
// any calls that occur after an error. At the end of a sequence of reads
// the Err field should be checked to see if any errors occurred.
type MessageReader struct {
	buf *bytes.Buffer
	Err error
}

func newMessageReader(buf *bytes.Buffer) *MessageReader {
	return &MessageReader{buf: buf}
}

func (r *MessageReader) ReadByte() (b byte) {
	if r.Err != nil {
		return
	}

	b, r.Err = r.buf.ReadByte()
	return
}

func (r *MessageReader) ReadInt16() (n int16) {
	if r.Err != nil {
		return
	}

	size := 2
	b := r.buf.Next(size)
	if len(b) != size {
		r.Err = fmt.Errorf("Unable to read %d bytes, only read %d", size, len(b))
	}

	return int16(binary.BigEndian.Uint16(b))
}

func (r *MessageReader) ReadInt32() (n int32) {
	if r.Err != nil {
		return
	}

	size := 4
	b := r.buf.Next(size)
	if len(b) != size {
		r.Err = fmt.Errorf("Unable to read %d bytes, only read %d", size, len(b))
	}

	return int32(binary.BigEndian.Uint32(b))
}

func (r *MessageReader) ReadInt64() (n int64) {
	if r.Err != nil {
		return
	}

	size := 8
	b := r.buf.Next(size)
	if len(b) != size {
		r.Err = fmt.Errorf("Unable to read %d bytes, only read %d", size, len(b))
	}

	return int64(binary.BigEndian.Uint64(b))
}

func (r *MessageReader) ReadOid() (oid Oid) {
	if r.Err != nil {
		return
	}

	size := 4
	b := r.buf.Next(size)
	if len(b) != size {
		r.Err = fmt.Errorf("Unable to read %d bytes, only read %d", size, len(b))
	}

	return Oid(binary.BigEndian.Uint32(b))
}

// ReadString reads a null terminated string
func (r *MessageReader) ReadCString() (s string) {
	if r.Err != nil {
		return
	}

	var b []byte
	b, r.Err = r.buf.ReadBytes(0)
	if r.Err != nil {
		return
	}

	return string(b[:len(b)-1])
}

// ReadString reads count bytes and returns as string
func (r *MessageReader) ReadString(count int32) (s string) {
	if r.Err != nil {
		return
	}

	size := int(count)
	b := r.buf.Next(size)
	if len(b) != size {
		r.Err = fmt.Errorf("Unable to read %d bytes, only read %d", size, len(b))
	}

	return string(b)
}
