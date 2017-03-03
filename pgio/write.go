package pgio

import (
	"encoding/binary"
	"io"
)

type Uint16Writer interface {
	WriteUint16(uint16) (n int, err error)
}

type Uint32Writer interface {
	WriteUint32(uint32) (n int, err error)
}

type Uint64Writer interface {
	WriteUint64(uint64) (n int, err error)
}

// WriteByte writes b to w.
func WriteByte(w io.Writer, b byte) error {
	if w, ok := w.(io.ByteWriter); ok {
		return w.WriteByte(b)
	}
	_, err := w.Write([]byte{b})
	return err
}

// WriteUint16 writes n to w in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Write if w provides a WriteUint16
// method.
func WriteUint16(w io.Writer, n uint16) (int, error) {
	if w, ok := w.(Uint16Writer); ok {
		return w.WriteUint16(n)
	}
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)
	return w.Write(b)
}

// WriteInt16 writes n to w in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Write if w provides a WriteUint16
// method.
func WriteInt16(w io.Writer, n int16) (int, error) {
	return WriteUint16(w, uint16(n))
}

// WriteUint32 writes n to w in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Write if w provides a WriteUint32
// method.
func WriteUint32(w io.Writer, n uint32) (int, error) {
	if w, ok := w.(Uint32Writer); ok {
		return w.WriteUint32(n)
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return w.Write(b)
}

// WriteInt32 writes n to w in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Write if w provides a WriteUint32
// method.
func WriteInt32(w io.Writer, n int32) (int, error) {
	return WriteUint32(w, uint32(n))
}

// WriteUint64 writes n to w in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Write if w provides a WriteUint64
// method.
func WriteUint64(w io.Writer, n uint64) (int, error) {
	if w, ok := w.(Uint64Writer); ok {
		return w.WriteUint64(n)
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return w.Write(b)
}

// WriteInt64 writes n to w in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Write if w provides a WriteUint64
// method.
func WriteInt64(w io.Writer, n int64) (int, error) {
	return WriteUint64(w, uint64(n))
}

// WriteCString writes s to w followed by a null byte.
func WriteCString(w io.Writer, s string) (int, error) {
	n, err := io.WriteString(w, s)
	if err != nil {
		return n, err
	}
	err = WriteByte(w, 0)
	if err != nil {
		return n, err
	}
	return n + 1, nil
}
