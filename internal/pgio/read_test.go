package pgio

import (
	"bytes"
	"errors"
	"testing"
)

func TestReaderReadsSequentially(t *testing.T) {
	r := NewReader([]byte{
		0x01,
		0x00, 0x02,
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
		0xde, 0xad,
	})

	if b := r.Byte(); b != 1 {
		t.Errorf("Byte() => %v, want 1", b)
	}
	if n := r.Uint16(); n != 2 {
		t.Errorf("Uint16() => %v, want 2", n)
	}
	if n := r.Uint32(); n != 3 {
		t.Errorf("Uint32() => %v, want 3", n)
	}
	if n := r.Uint64(); n != 4 {
		t.Errorf("Uint64() => %v, want 4", n)
	}
	if b := r.Bytes(2); !bytes.Equal(b, []byte{0xde, 0xad}) {
		t.Errorf("Bytes(2) => %v, want [de ad]", b)
	}
	if err := r.Finish(); err != nil {
		t.Errorf("Finish() => %v, want nil", err)
	}
}

func TestReaderNegativeIntegers(t *testing.T) {
	r := NewReader([]byte{
		0xff, 0xff,
		0xff, 0xff, 0xff, 0xfe,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd,
	})

	if n := r.Int16(); n != -1 {
		t.Errorf("Int16() => %v, want -1", n)
	}
	if n := r.Int32(); n != -2 {
		t.Errorf("Int32() => %v, want -2", n)
	}
	if n := r.Int64(); n != -3 {
		t.Errorf("Int64() => %v, want -3", n)
	}
	if err := r.Finish(); err != nil {
		t.Errorf("Finish() => %v, want nil", err)
	}
}

func TestReaderInsufficientBytesIsSticky(t *testing.T) {
	r := NewReader([]byte{0x00, 0x01})

	if n := r.Uint32(); n != 0 {
		t.Errorf("short Uint32() => %v, want 0", n)
	}
	if !errors.Is(r.Err(), ErrInsufficientBytes) {
		t.Errorf("Err() => %v, want ErrInsufficientBytes", r.Err())
	}

	// All subsequent reads return zero values without advancing.
	if b := r.Byte(); b != 0 {
		t.Errorf("Byte() after error => %v, want 0", b)
	}
	if b := r.Bytes(1); b != nil {
		t.Errorf("Bytes(1) after error => %v, want nil", b)
	}
	if data, null := r.Value(); data != nil || null {
		t.Errorf("Value() after error => %v, %v, want nil, false", data, null)
	}
	if !errors.Is(r.Finish(), ErrInsufficientBytes) {
		t.Errorf("Finish() => %v, want ErrInsufficientBytes", r.Finish())
	}
}

func TestReaderEmptySource(t *testing.T) {
	for name, read := range map[string]func(*Reader){
		"Byte":   func(r *Reader) { r.Byte() },
		"Uint16": func(r *Reader) { r.Uint16() },
		"Uint32": func(r *Reader) { r.Uint32() },
		"Uint64": func(r *Reader) { r.Uint64() },
		"Bytes":  func(r *Reader) { r.Bytes(1) },
		"Count":  func(r *Reader) { r.Count(1) },
		"Value":  func(r *Reader) { r.Value() },
	} {
		r := NewReader(nil)
		read(r)
		if !errors.Is(r.Err(), ErrInsufficientBytes) {
			t.Errorf("%s on empty source: Err() => %v, want ErrInsufficientBytes", name, r.Err())
		}
	}
}

func TestReaderBytesNegativeCount(t *testing.T) {
	r := NewReader([]byte{0x00})
	if b := r.Bytes(-1); b != nil {
		t.Errorf("Bytes(-1) => %v, want nil", b)
	}
	if r.Err() == nil {
		t.Error("Bytes(-1): Err() => nil, want error")
	}
}

func TestReaderValue(t *testing.T) {
	// Two values: 3-byte "abc", then NULL.
	r := NewReader([]byte{
		0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c',
		0xff, 0xff, 0xff, 0xff,
	})

	data, null := r.Value()
	if null || !bytes.Equal(data, []byte("abc")) {
		t.Errorf("Value() => %v, %v, want \"abc\", false", data, null)
	}
	data, null = r.Value()
	if !null || data != nil {
		t.Errorf("Value() => %v, %v, want nil, true", data, null)
	}
	if err := r.Finish(); err != nil {
		t.Errorf("Finish() => %v, want nil", err)
	}
}

func TestReaderValueInvalidLength(t *testing.T) {
	// -2 is not a valid length; only -1 (NULL) is.
	r := NewReader([]byte{0xff, 0xff, 0xff, 0xfe})
	if data, null := r.Value(); data != nil || null {
		t.Errorf("Value() => %v, %v, want nil, false", data, null)
	}
	if r.Err() == nil {
		t.Error("Value() with length -2: Err() => nil, want error")
	}
}

func TestReaderValueTruncatedData(t *testing.T) {
	r := NewReader([]byte{0x00, 0x00, 0x00, 0x05, 'a', 'b'})
	if data, _ := r.Value(); data != nil {
		t.Errorf("Value() => %v, want nil", data)
	}
	if !errors.Is(r.Err(), ErrInsufficientBytes) {
		t.Errorf("Err() => %v, want ErrInsufficientBytes", r.Err())
	}
}

func TestReaderCount(t *testing.T) {
	// Count 2 with 8 bytes remaining and minElemSize 4 is valid.
	r := NewReader([]byte{0x00, 0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0, 0, 0})
	if n := r.Count(4); n != 2 || r.Err() != nil {
		t.Errorf("Count(4) => %v (err %v), want 2", n, r.Err())
	}

	// Count 3 with 8 bytes remaining and minElemSize 4 exceeds the source.
	r = NewReader([]byte{0x00, 0x00, 0x00, 0x03, 0, 0, 0, 0, 0, 0, 0, 0})
	if n := r.Count(4); n != 0 {
		t.Errorf("oversized Count(4) => %v, want 0", n)
	}
	if r.Err() == nil {
		t.Error("oversized Count(4): Err() => nil, want error")
	}

	// Negative counts are invalid.
	r = NewReader([]byte{0xff, 0xff, 0xff, 0xff})
	if n := r.Count(1); n != 0 {
		t.Errorf("negative Count(1) => %v, want 0", n)
	}
	if r.Err() == nil {
		t.Error("negative Count(1): Err() => nil, want error")
	}
}

func TestReaderFinishTrailingBytes(t *testing.T) {
	r := NewReader([]byte{0x01, 0x02})
	r.Byte()
	if err := r.Finish(); err == nil {
		t.Error("Finish() with unread bytes => nil, want error")
	}
}
