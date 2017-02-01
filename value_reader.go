package pgx

import (
	"errors"
	"fmt"
)

// ValueReader is used by the Scanner interface to decode values.
type ValueReader struct {
	mr                  *msgReader
	fd                  *FieldDescription
	valueBytesRemaining int32
	err                 error
}

// Err returns any error that the ValueReader has experienced
func (r *ValueReader) Err() error {
	return r.err
}

// Fatal tells r that a Fatal error has occurred
func (r *ValueReader) Fatal(err error) {
	r.err = err
}

// Len returns the number of unread bytes
func (r *ValueReader) Len() int32 {
	return r.valueBytesRemaining
}

// Type returns the *FieldDescription of the value
func (r *ValueReader) Type() *FieldDescription {
	return r.fd
}

func (r *ValueReader) ReadByte() byte {
	if r.err != nil {
		return 0
	}

	r.valueBytesRemaining--
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return 0
	}

	return r.mr.readByte()
}

func (r *ValueReader) ReadInt16() int16 {
	if r.err != nil {
		return 0
	}

	r.valueBytesRemaining -= 2
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return 0
	}

	return r.mr.readInt16()
}

func (r *ValueReader) ReadUint16() uint16 {
	if r.err != nil {
		return 0
	}

	r.valueBytesRemaining -= 2
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return 0
	}

	return r.mr.readUint16()
}

func (r *ValueReader) ReadInt32() int32 {
	if r.err != nil {
		return 0
	}

	r.valueBytesRemaining -= 4
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return 0
	}

	return r.mr.readInt32()
}

func (r *ValueReader) ReadUint32() uint32 {
	if r.err != nil {
		return 0
	}

	r.valueBytesRemaining -= 4
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return 0
	}

	return r.mr.readUint32()
}

func (r *ValueReader) ReadInt64() int64 {
	if r.err != nil {
		return 0
	}

	r.valueBytesRemaining -= 8
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return 0
	}

	return r.mr.readInt64()
}

func (r *ValueReader) ReadOid() Oid {
	return Oid(r.ReadUint32())
}

// ReadString reads count bytes and returns as string
func (r *ValueReader) ReadString(count int32) string {
	if r.err != nil {
		return ""
	}

	r.valueBytesRemaining -= count
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return ""
	}

	return r.mr.readString(count)
}

// ReadUuid reads count bytes and returns the formatted uuid
func (r *ValueReader) ReadUuid(count int32) string {
	if r.err != nil {
		return ""
	}
	if count != 16 {
		r.Fatal(errors.New("unexpected UUID length"))
		return ""
	}

	r.valueBytesRemaining -= count
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return ""
	}

	v := r.mr.readBytes(count)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", v[:4], v[4:6], v[6:8], v[8:10], v[10:])
}

// ReadBytes reads count bytes and returns as []byte
func (r *ValueReader) ReadBytes(count int32) []byte {
	if r.err != nil {
		return nil
	}

	if count < 0 {
		r.Fatal(errors.New("count must not be negative"))
		return nil
	}

	r.valueBytesRemaining -= count
	if r.valueBytesRemaining < 0 {
		r.Fatal(errors.New("read past end of value"))
		return nil
	}

	return r.mr.readBytes(count)
}
