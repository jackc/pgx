package pgx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"

	"github.com/jackc/pgx/chunkreader"
)

// msgReader is a helper that reads values from a PostgreSQL message.
type msgReader struct {
	cr        *chunkreader.ChunkReader
	msgType   byte
	msgBody   []byte
	rp        int // read position
	err       error
	log       func(lvl int, msg string, ctx ...interface{})
	shouldLog func(lvl int) bool
}

// fatal tells rc that a Fatal error has occurred
func (r *msgReader) fatal(err error) {
	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.fatal", "error", err, "msgType", r.msgType, "msgBody", r.msgBody, "rp", r.rp)
	}
	r.err = err
}

// rxMsg reads the type and size of the next message.
func (r *msgReader) rxMsg() (byte, error) {
	if r.err != nil {
		return 0, r.err
	}

	header, err := r.cr.Next(5)
	if err != nil {
		if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
			r.fatal(err)
		}
		return 0, err
	}

	r.msgType = header[0]
	bodyLen := int(binary.BigEndian.Uint32(header[1:])) - 4

	r.msgBody, err = r.cr.Next(bodyLen)
	if err != nil {
		if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
			r.fatal(err)
		}
		return 0, err
	}

	r.rp = 0

	return r.msgType, nil
}

func (r *msgReader) readByte() byte {
	if r.err != nil {
		return 0
	}

	if len(r.msgBody)-r.rp < 1 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	b := r.msgBody[r.rp]
	r.rp++

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readByte", "value", b, "byteAsString", string(b), "msgType", r.msgType, "rp", r.rp)
	}

	return b
}

func (r *msgReader) readInt16() int16 {
	if r.err != nil {
		return 0
	}

	if len(r.msgBody)-r.rp < 2 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	n := int16(binary.BigEndian.Uint16(r.msgBody[r.rp:]))
	r.rp += 2

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt16", "value", n, "msgType", r.msgType, "rp", r.rp)
	}

	return n
}

func (r *msgReader) readInt32() int32 {
	if r.err != nil {
		return 0
	}

	if len(r.msgBody)-r.rp < 4 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	n := int32(binary.BigEndian.Uint32(r.msgBody[r.rp:]))
	r.rp += 4

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt32", "value", n, "msgType", r.msgType, "rp", r.rp)
	}

	return n
}

func (r *msgReader) readUint16() uint16 {
	if r.err != nil {
		return 0
	}

	if len(r.msgBody)-r.rp < 2 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	n := binary.BigEndian.Uint16(r.msgBody[r.rp:])
	r.rp += 2

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readUint16", "value", n, "msgType", r.msgType, "rp", r.rp)
	}

	return n
}

func (r *msgReader) readUint32() uint32 {
	if r.err != nil {
		return 0
	}

	if len(r.msgBody)-r.rp < 4 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	n := binary.BigEndian.Uint32(r.msgBody[r.rp:])
	r.rp += 4

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readUint32", "value", n, "msgType", r.msgType, "rp", r.rp)
	}

	return n
}

func (r *msgReader) readInt64() int64 {
	if r.err != nil {
		return 0
	}

	if len(r.msgBody)-r.rp < 8 {
		r.fatal(errors.New("read past end of message"))
		return 0
	}

	n := int64(binary.BigEndian.Uint64(r.msgBody[r.rp:]))
	r.rp += 8

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readInt64", "value", n, "msgType", r.msgType, "rp", r.rp)
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

	nullIdx := bytes.IndexByte(r.msgBody[r.rp:], 0)
	if nullIdx == -1 {
		r.fatal(errors.New("null terminated string not found"))
		return ""
	}

	s := string(r.msgBody[r.rp : r.rp+nullIdx])
	r.rp += nullIdx + 1

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readCString", "value", s, "msgType", r.msgType, "rp", r.rp)
	}

	return s
}

// readString reads count bytes and returns as string
func (r *msgReader) readString(countI32 int32) string {
	if r.err != nil {
		return ""
	}

	count := int(countI32)

	if len(r.msgBody)-r.rp < count {
		r.fatal(errors.New("read past end of message"))
		return ""
	}

	s := string(r.msgBody[r.rp : r.rp+count])
	r.rp += count

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readString", "value", s, "msgType", r.msgType, "rp", r.rp)
	}

	return s
}

// readBytes reads count bytes and returns as []byte
func (r *msgReader) readBytes(countI32 int32) []byte {
	if r.err != nil {
		return nil
	}

	count := int(countI32)

	if len(r.msgBody)-r.rp < count {
		r.fatal(errors.New("read past end of message"))
		return nil
	}

	b := r.msgBody[r.rp : r.rp+count]
	r.rp += count

	r.cr.KeepLast()

	if r.shouldLog(LogLevelTrace) {
		r.log(LogLevelTrace, "msgReader.readBytes", "value", b, r.msgType, "rp", r.rp)
	}

	return b
}
