package pgx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// msgReader is a helper that reads values from a PostgreSQL message.
type msgReader struct {
	reader    *bytes.Buffer // using Buffer instead of Reader because of ReadBytes
	err       error
	log       func(lvl int, msg string, ctx ...interface{})
	shouldLog func(lvl int) bool
}

// Err returns any error that the msgReader has experienced
func (r *msgReader) Err() error {
	return r.err
}

// fatal tells r that a Fatal error has occurred
func (r *msgReader) fatal(err error) {
	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.fatal", "error", err, "reader.Len", r.reader.Len())
	}
	r.err = err
}

func (r *msgReader) readByte() byte {
	if r.err != nil {
		return 0
	}

	b, err := r.reader.ReadByte()
	if err != nil {
		r.fatal(err)
		return 0
	}

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readByte", "value", b, "byteAsString", string(b), "reader.Len", r.reader.Len())
	}

	return b
}

func (r *msgReader) readInt16() int16 {
	if r.err != nil {
		return 0
	}

	buf := make([]byte, 2)

	_, err := r.reader.Read(buf)
	if err != nil {
		r.fatal(err)
		return 0
	}

	n := int16(binary.BigEndian.Uint16(buf))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt16", "value", n, "reader.Len", r.reader.Len())
	}

	return n
}

func (r *msgReader) readInt32() int32 {
	if r.err != nil {
		return 0
	}

	buf := make([]byte, 4)

	_, err := r.reader.Read(buf)
	if err != nil {
		r.fatal(err)
		return 0
	}

	n := int32(binary.BigEndian.Uint32(buf))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt32", "value", n, "reader.Len", r.reader.Len())
	}

	return n
}

func (r *msgReader) readUint16() uint16 {
	if r.err != nil {
		return 0
	}

	buf := make([]byte, 2)

	_, err := r.reader.Read(buf)
	if err != nil {
		r.fatal(err)
		return 0
	}

	n := uint16(binary.BigEndian.Uint16(buf))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readUint16", "value", n, "reader.Len", r.reader.Len())
	}

	return n
}

func (r *msgReader) readUint32() uint32 {
	if r.err != nil {
		return 0
	}

	buf := make([]byte, 4)

	_, err := r.reader.Read(buf)
	if err != nil {
		r.fatal(err)
		return 0
	}

	n := uint32(binary.BigEndian.Uint32(buf))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readUint32", "value", n, "reader.Len", r.reader.Len())
	}

	return n
}

func (r *msgReader) readInt64() int64 {
	if r.err != nil {
		return 0
	}

	buf := make([]byte, 8)

	_, err := r.reader.Read(buf)
	if err != nil {
		r.fatal(err)
		return 0
	}

	n := int64(binary.BigEndian.Uint64(buf))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt64", "value", n, "reader.Len", r.reader.Len())
	}

	return n
}

func (r *msgReader) readOID() OID {
	return OID(r.readInt32())
}

// readCString reads a null terminated string
func (r *msgReader) readCString() string {
	if r.err != nil {
		return ""
	}

	buf, err := r.reader.ReadBytes(0)
	if err != nil {
		r.fatal(err)
		return ""
	}

	s := string(buf[0 : len(buf)-1])

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readCString", "value", s, "reader.Len", r.reader.Len())
	}

	return s
}

// readString reads count bytes and returns as string
func (r *msgReader) readString(countI32 int32) string {
	if r.err != nil {
		return ""
	}

	if r.reader.Len() < int(countI32) {
		r.fatal(errors.New("read past end of message"))
		return ""
	}

	s := string(r.reader.Next(int(countI32)))

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readString", "value", s, "reader.Len", r.reader.Len())
	}

	return s
}

// readBytes reads count bytes and returns as []byte
func (r *msgReader) readBytes(count int32) []byte {
	if r.err != nil {
		return nil
	}

	buf := make([]byte, int(count))

	_, err := io.ReadFull(r.reader, buf)
	if err != nil {
		r.fatal(err)
		return nil
	}

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readBytes", "value", buf, "reader.Len", r.reader.Len())
	}

	return buf
}
