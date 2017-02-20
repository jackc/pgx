package pgtype

import (
	"encoding/binary"
	"io"
)

type uint16Reader interface {
	ReadUint16() (n uint16, err error)
}

type uint32Reader interface {
	ReadUint32() (n uint32, err error)
}

type uint64Reader interface {
	ReadUint64() (n uint64, err error)
}

// ReadByte reads a byte from r.
func ReadByte(r io.Reader) (byte, error) {
	if r, ok := r.(io.ByteReader); ok {
		return r.ReadByte()
	}

	buf := make([]byte, 1)
	_, err := r.Read(buf)
	return buf[0], err
}

// ReadUint16 reads an uint16 from r in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Read if r provides a ReadUint16
// method.
func ReadUint16(r io.Reader) (uint16, error) {
	if r, ok := r.(uint16Reader); ok {
		return r.ReadUint16()
	}

	buf := make([]byte, 2)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint16(buf), nil
}

// ReadInt16 reads an int16 r in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Read if r provides a ReadUint16
// method.
func ReadInt16(r io.Reader) (int16, error) {
	n, err := ReadUint16(r)
	return int16(n), err
}

// ReadUint32 reads an uint32 r in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Read if r provides a ReadUint32
// method.
func ReadUint32(r io.Reader) (uint32, error) {
	if r, ok := r.(uint32Reader); ok {
		return r.ReadUint32()
	}

	buf := make([]byte, 4)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf), nil
}

// ReadInt32 reads an int32 r in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Read if r provides a ReadUint32
// method.
func ReadInt32(r io.Reader) (int32, error) {
	n, err := ReadUint32(r)
	return int32(n), err
}

// ReadUint64 reads an uint64 r in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Read if r provides a ReadUint64
// method.
func ReadUint64(r io.Reader) (uint64, error) {
	if r, ok := r.(uint64Reader); ok {
		return r.ReadUint64()
	}

	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf), nil
}

// ReadInt64 reads an int64 r in PostgreSQL wire format (network byte order). This
// may be more efficient than directly using Read if r provides a ReadUint64
// method.
func ReadInt64(r io.Reader) (int64, error) {
	n, err := ReadUint64(r)
	return int64(n), err
}
