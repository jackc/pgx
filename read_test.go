package pgio

import (
	"testing"
)

func TestNextByte(t *testing.T) {
	buf := []byte{42, 1}
	var b byte
	buf, b = NextByte(buf)
	if b != 42 {
		t.Errorf("NextByte(buf) => %v, want %v", b, 42)
	}
	buf, b = NextByte(buf)
	if b != 1 {
		t.Errorf("NextByte(buf) => %v, want %v", b, 1)
	}
}

func TestNextUint16(t *testing.T) {
	buf := []byte{0, 42, 0, 1}
	var n uint16
	buf, n = NextUint16(buf)
	if n != 42 {
		t.Errorf("NextUint16(buf) => %v, want %v", n, 42)
	}
	buf, n = NextUint16(buf)
	if n != 1 {
		t.Errorf("NextUint16(buf) => %v, want %v", n, 1)
	}
}

func TestNextUint32(t *testing.T) {
	buf := []byte{0, 0, 0, 42, 0, 0, 0, 1}
	var n uint32
	buf, n = NextUint32(buf)
	if n != 42 {
		t.Errorf("NextUint32(buf) => %v, want %v", n, 42)
	}
	buf, n = NextUint32(buf)
	if n != 1 {
		t.Errorf("NextUint32(buf) => %v, want %v", n, 1)
	}
}

func TestNextUint64(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 1}
	var n uint64
	buf, n = NextUint64(buf)
	if n != 42 {
		t.Errorf("NextUint64(buf) => %v, want %v", n, 42)
	}
	buf, n = NextUint64(buf)
	if n != 1 {
		t.Errorf("NextUint64(buf) => %v, want %v", n, 1)
	}
}
