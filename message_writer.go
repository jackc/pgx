package pgx

import (
	"bytes"
	"encoding/binary"
)

// MessageWriter is a helper for producing messages to send to PostgreSQL.
// To avoid verbose error handling it internally records errors and no-ops
// any calls that occur after an error. At the end of a sequence of writes
// the Err field should be checked to see if any errors occurred.
type MessageWriter struct {
	buf *bytes.Buffer
	Err error
}

func newMessageWriter(buf *bytes.Buffer) *MessageWriter {
	return &MessageWriter{buf: buf}
}

// WriteCString writes a null-terminated string.
func (w *MessageWriter) WriteCString(s string) {
	if w.Err != nil {
		return
	}
	if _, w.Err = w.buf.WriteString(s); w.Err != nil {
		return
	}
	w.Err = w.buf.WriteByte(0)
}

// WriteString writes a string without a null terminator.
func (w *MessageWriter) WriteString(s string) {
	if w.Err != nil {
		return
	}
	if _, w.Err = w.buf.WriteString(s); w.Err != nil {
		return
	}
}

func (w *MessageWriter) WriteByte(b byte) {
	if w.Err != nil {
		return
	}

	w.Err = w.buf.WriteByte(b)
}

// Write writes data in the network byte order. data can be an integer type,
// float type, or byte slice.
func (w *MessageWriter) Write(data interface{}) {
	if w.Err != nil {
		return
	}

	w.Err = binary.Write(w.buf, binary.BigEndian, data)
}
