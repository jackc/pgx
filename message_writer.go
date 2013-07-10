package pgx

import (
	"bytes"
	"encoding/binary"
)

type messageWriter struct {
	buf *bytes.Buffer
	err error
}

func newMessageWriter(buf *bytes.Buffer) *messageWriter {
	return &messageWriter{buf: buf}
}

func (w *messageWriter) writeCString(s string) {
	if w.err != nil {
		return
	}
	if _, w.err = w.buf.WriteString(s); w.err != nil {
		return
	}
	w.err = w.buf.WriteByte(0)
}

func (w *messageWriter) writeString(s string) {
	if w.err != nil {
		return
	}
	if _, w.err = w.buf.WriteString(s); w.err != nil {
		return
	}
}

func (w *messageWriter) writeByte(b byte) {
	if w.err != nil {
		return
	}

	w.err = w.buf.WriteByte(b)
}

func (w *messageWriter) write(data interface{}) {
	if w.err != nil {
		return
	}

	w.err = binary.Write(w.buf, binary.BigEndian, data)
}
