package pgx

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// msgReader is a helper that reads values from a PostgreSQL message.
type msgReader struct {
	reader            *bufio.Reader
	buf               [128]byte
	msgBytesRemaining int32
	err               error
	log               func(lvl int, msg string, ctx ...interface{})
	shouldLog         func(lvl int) bool
}

// Err returns any error that the msgReader has experienced
func (r *msgReader) Err() error {
	return r.err
}

// fatal tells r that a Fatal error has occurred
func (r *msgReader) fatal(err error) {
	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.fatal", "error", err, "msgBytesRemaining", r.msgBytesRemaining)
	}
	r.err = err
}

// rxMsg reads the type and size of the next message.
func (r *msgReader) rxMsg() (byte, error) {
	if r.err != nil {
		return 0, r.err
	}

	if r.msgBytesRemaining > 0 {
		if r.shouldLog(LogLevelTrace) {
			r.log(LogLevelTrace, "msgReader.rxMsg discarding unread previous message", "msgBytesRemaining", r.msgBytesRemaining)
		}

		_, err := r.reader.Discard(int(r.msgBytesRemaining))
		if err != nil {
			return 0, err
		}
	}

	b := r.buf[0:5]
	_, err := io.ReadFull(r.reader, b)
	r.msgBytesRemaining = int32(binary.BigEndian.Uint32(b[1:])) - 4
	return b[0], err
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

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readByte", "value", b, "byteAsString", string(b), "msgBytesRemaining", r.msgBytesRemaining)
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

	n := int16(binary.BigEndian.Uint16(b))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt16", "value", n, "msgBytesRemaining", r.msgBytesRemaining)
	}

	return n
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

	n := int32(binary.BigEndian.Uint32(b))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt32", "value", n, "msgBytesRemaining", r.msgBytesRemaining)
	}

	return n
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

	n := int64(binary.BigEndian.Uint64(b))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt64", "value", n, "msgBytesRemaining", r.msgBytesRemaining)
	}

	return n
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

	s := string(b[0 : len(b)-1])

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readCString", "value", s, "msgBytesRemaining", r.msgBytesRemaining)
	}

	return s
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

	s := string(b)

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readString", "value", s, "msgBytesRemaining", r.msgBytesRemaining)
	}

	return s
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

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readBytes", "value", b, "msgBytesRemaining", r.msgBytesRemaining)
	}

	return b
}
