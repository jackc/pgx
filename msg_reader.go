package pgx

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
)

// msgReader is a helper that reads values from a PostgreSQL message.
type msgReader struct {
	reader            *bufio.Reader
	buf               [128]byte
	msgBytesRemaining int32
	err               error
}

// Err returns any error that the msgReader has experienced
func (r *msgReader) Err() error {
	return r.err
}

// fatal tells r that a Fatal error has occurred
func (r *msgReader) fatal(err error) {
	r.err = err
}

// rxMsg reads the type and size of the next message.
func (r *msgReader) rxMsg() (t byte, err error) {
	if r.err != nil {
		return 0, err
	}

	if r.msgBytesRemaining > 0 {
		io.CopyN(ioutil.Discard, r.reader, int64(r.msgBytesRemaining))
	}

	t, err = r.reader.ReadByte()
	b := r.buf[0:4]
	_, err = io.ReadFull(r.reader, b)
	r.msgBytesRemaining = int32(binary.BigEndian.Uint32(b)) - 4
	return t, err
}

func (r *msgReader) readByte() byte {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 1
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b, err := r.reader.ReadByte()
	if err != nil {
		r.fatal(err)
		return 0
	}

	return b
}

func (r *msgReader) readInt16() int16 {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 2
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.buf[0:2]
	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return 0
	}

	return int16(binary.BigEndian.Uint16(b))
}

func (r *msgReader) readInt32() int32 {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 4
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.buf[0:4]
	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return 0
	}

	return int32(binary.BigEndian.Uint32(b))
}

func (r *msgReader) readInt64() int64 {
	if r.err != nil {
		return 0
	}

	r.msgBytesRemaining -= 8
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.buf[0:8]
	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return 0
	}

	return int64(binary.BigEndian.Uint64(b))
}

func (r *msgReader) readOid() Oid {
	return Oid(r.readInt32())
}

// readCString reads a null terminated string
func (r *msgReader) readCString() string {
	if r.err != nil {
		return ""
	}

	b, err := r.reader.ReadBytes(0)
	if err != nil {
		r.fatal(err)
		return ""
	}

	r.msgBytesRemaining -= int32(len(b))
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return ""
	}

	return string(b[0 : len(b)-1])
}

// readString reads count bytes and returns as string
func (r *msgReader) readString(count int32) string {
	if r.err != nil {
		return ""
	}

	r.msgBytesRemaining -= count
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return ""
	}

	var b []byte
	if count <= int32(len(r.buf)) {
		b = r.buf[0:int(count)]
	} else {
		b = make([]byte, int(count))
	}

	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return ""
	}

	return string(b)
}

// readBytes reads count bytes and returns as []byte
func (r *msgReader) readBytes(count int32) []byte {
	if r.err != nil {
		return nil
	}

	r.msgBytesRemaining -= count
	if r.msgBytesRemaining < 0 {
		r.fatal(errors.New("read past end of message"))
		return nil
	}

	b := make([]byte, int(count))

	_, err := io.ReadFull(r.reader, b)
	if err != nil {
		r.fatal(err)
		return nil
	}

	return b
}
