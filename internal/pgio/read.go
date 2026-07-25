package pgio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrInsufficientBytes is wrapped by all Reader errors caused by a read past
// the end of the source.
var ErrInsufficientBytes = errors.New("insufficient bytes")

// Reader is a bounds-checked reader for the PostgreSQL binary format. It is
// designed so that decoders of untrusted input cannot forget a length check:
// every read validates against the remaining bytes, and the first failure
// sticks. After a failure all subsequent reads return zero values, so a
// decoder can read an entire structure without intermediate error checks and
// inspect Err or Finish once at the end.
//
// Reads never panic. A decoder that branches on a value it just read (e.g. an
// element count used to size an allocation) should check Err before acting on
// the value.
type Reader struct {
	s   []byte
	rp  int
	err error
}

func NewReader(s []byte) *Reader {
	return &Reader{s: s}
}

func (r *Reader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

// need reports whether n more bytes are available, recording an error if not.
func (r *Reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if len(r.s)-r.rp < n {
		r.fail(fmt.Errorf("%w: %d needed at offset %d, %d remain", ErrInsufficientBytes, n, r.rp, len(r.s)-r.rp))
		return false
	}
	return true
}

// Err returns the first error encountered, if any.
func (r *Reader) Err() error {
	return r.err
}

// Remaining returns the number of unread bytes.
func (r *Reader) Remaining() int {
	return len(r.s) - r.rp
}

func (r *Reader) Byte() byte {
	if !r.need(1) {
		return 0
	}
	b := r.s[r.rp]
	r.rp += 1
	return b
}

func (r *Reader) Uint16() uint16 {
	if !r.need(2) {
		return 0
	}
	n := binary.BigEndian.Uint16(r.s[r.rp:])
	r.rp += 2
	return n
}

func (r *Reader) Int16() int16 {
	return int16(r.Uint16())
}

func (r *Reader) Uint32() uint32 {
	if !r.need(4) {
		return 0
	}
	n := binary.BigEndian.Uint32(r.s[r.rp:])
	r.rp += 4
	return n
}

func (r *Reader) Int32() int32 {
	return int32(r.Uint32())
}

func (r *Reader) Uint64() uint64 {
	if !r.need(8) {
		return 0
	}
	n := binary.BigEndian.Uint64(r.s[r.rp:])
	r.rp += 8
	return n
}

func (r *Reader) Int64() int64 {
	return int64(r.Uint64())
}

// Bytes reads the next n bytes. The returned slice aliases the source; it is
// not a copy.
func (r *Reader) Bytes(n int) []byte {
	if n < 0 {
		r.fail(fmt.Errorf("invalid byte count %d at offset %d", n, r.rp))
		return nil
	}
	if !r.need(n) {
		return nil
	}
	b := r.s[r.rp : r.rp+n]
	r.rp += n
	return b
}

// Count reads an int32 element count and validates it against the remaining
// bytes: the count must be non-negative, and since each element occupies at
// least minElemSize bytes, count*minElemSize must not exceed the remaining
// message. This bounds allocations sized from the count against a malicious
// or corrupt message claiming a huge count. Returns 0 on any failure.
func (r *Reader) Count(minElemSize int) int {
	offset := r.rp
	count := int(r.Int32())
	if r.err != nil {
		return 0
	}
	if count < 0 {
		r.fail(fmt.Errorf("invalid element count %d at offset %d", count, offset))
		return 0
	}
	if minElemSize < 1 {
		minElemSize = 1
	}
	if count > r.Remaining()/minElemSize {
		r.fail(fmt.Errorf("element count %d at offset %d exceeds %d remaining bytes", count, offset, r.Remaining()))
		return 0
	}
	return count
}

// Value reads an int32 length followed by that many bytes — the standard
// PostgreSQL binary representation of a value. A length of -1 means NULL and
// returns (nil, true). Any other negative length is an error. The returned
// slice aliases the source; null is only meaningful if Err returns nil.
func (r *Reader) Value() (data []byte, null bool) {
	offset := r.rp
	length := r.Int32()
	if r.err != nil {
		return nil, false
	}
	if length == -1 {
		return nil, true
	}
	if length < 0 {
		r.fail(fmt.Errorf("invalid value length %d at offset %d", length, offset))
		return nil, false
	}
	return r.Bytes(int(length)), false
}

// Finish returns the first error encountered, or an error if unread bytes
// remain. Decoders that must consume the entire source should end with Finish.
func (r *Reader) Finish() error {
	if r.err != nil {
		return r.err
	}
	if r.rp != len(r.s) {
		return fmt.Errorf("%d unexpected trailing bytes at offset %d", len(r.s)-r.rp, r.rp)
	}
	return nil
}
