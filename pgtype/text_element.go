package pgtype

import (
	"bytes"
	"errors"
	"io"

	"github.com/jackc/pgx/pgio"
)

// TextElementWriter is a wrapper that makes TextEncoders composable into other
// TextEncoders. TextEncoder first writes the length of the subsequent value.
// This is not necessary when the value is part of another value such as an
// array. TextElementWriter requires one int32 to be written first which it
// ignores. No other integer writes are valid.
type TextElementWriter struct {
	w                   io.Writer
	lengthHeaderIgnored bool
}

func NewTextElementWriter(w io.Writer) *TextElementWriter {
	return &TextElementWriter{w: w}
}

func (w *TextElementWriter) WriteUint16(n uint16) (int, error) {
	return 0, errors.New("WriteUint16 should never be called on TextElementWriter")
}

func (w *TextElementWriter) WriteUint32(n uint32) (int, error) {
	if !w.lengthHeaderIgnored {
		w.lengthHeaderIgnored = true

		if int32(n) == -1 {
			return io.WriteString(w.w, "NULL")
		}

		return 4, nil
	}

	return 0, errors.New("WriteUint32 should only be called once on TextElementWriter")
}

func (w *TextElementWriter) WriteUint64(n uint64) (int, error) {
	if w.lengthHeaderIgnored {
		return pgio.WriteUint64(w.w, n)
	}

	return 0, errors.New("WriteUint64 should never be called on TextElementWriter")
}

func (w *TextElementWriter) Write(buf []byte) (int, error) {
	if w.lengthHeaderIgnored {
		return w.w.Write(buf)
	}

	return 0, errors.New("int32 must be written first")
}

func (w *TextElementWriter) Reset() {
	w.lengthHeaderIgnored = false
}

// TextElementReader is a wrapper that makes TextDecoders composable into other
// TextDecoders. TextEncoders first read the length of the subsequent value.
// This length value is not present when the value is part of another value such
// as an array. TextElementReader provides a substitute length value from the
// length of the string. No other integer reads are valid. Each time DecodeText
// is called with a TextElementReader as the source the TextElementReader must
// first have Reset called with the new element string data.
type TextElementReader struct {
	buf                 *bytes.Buffer
	lengthHeaderIgnored bool
}

func NewTextElementReader(r io.Reader) *TextElementReader {
	return &TextElementReader{buf: &bytes.Buffer{}}
}

func (r *TextElementReader) ReadUint16() (uint16, error) {
	return 0, errors.New("ReadUint16 should never be called on TextElementReader")
}

func (r *TextElementReader) ReadUint32() (uint32, error) {
	if !r.lengthHeaderIgnored {
		r.lengthHeaderIgnored = true
		if r.buf.String() == "NULL" {
			n32 := int32(-1)
			return uint32(n32), nil
		}
		return uint32(r.buf.Len()), nil
	}

	return 0, errors.New("ReadUint32 should only be called once on TextElementReader")
}

func (r *TextElementReader) WriteUint64(n uint64) (int, error) {
	return 0, errors.New("ReadUint64 should never be called on TextElementReader")
}

func (r *TextElementReader) Read(buf []byte) (int, error) {
	if r.lengthHeaderIgnored {
		return r.buf.Read(buf)
	}

	return 0, errors.New("int32 must be read first")
}

func (r *TextElementReader) Reset(s string) {
	r.lengthHeaderIgnored = false
	r.buf.Reset()
	r.buf.WriteString(s)
}
