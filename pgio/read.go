package pgio

import (
	"bytes"
	"encoding/binary"
)

func NextByte(buf []byte) ([]byte, byte) {
	b := buf[0]
	return buf[1:], b
}

func NextUint16(buf []byte) ([]byte, uint16) {
	n := binary.BigEndian.Uint16(buf)
	return buf[2:], n
}

func NextUint32(buf []byte) ([]byte, uint32) {
	n := binary.BigEndian.Uint32(buf)
	return buf[4:], n
}

func NextUint64(buf []byte) ([]byte, uint64) {
	n := binary.BigEndian.Uint64(buf)
	return buf[8:], n
}

func NextInt16(buf []byte) ([]byte, int16) {
	buf, n := NextUint16(buf)
	return buf, int16(n)
}

func NextInt32(buf []byte) ([]byte, int32) {
	buf, n := NextUint32(buf)
	return buf, int32(n)
}

func NextInt64(buf []byte) ([]byte, int64) {
	buf, n := NextUint64(buf)
	return buf, int64(n)
}

func NextCString(buf []byte) ([]byte, string, bool) {
	idx := bytes.IndexByte(buf, 0)
	if idx < 0 {
		return buf, "", false
	}
	cstring := string(buf[:idx])
	buf = buf[:idx+1]
	return buf, cstring, true
}
