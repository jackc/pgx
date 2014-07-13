package pgx

import (
	"encoding/binary"
)

const (
	protocolVersionNumber = 196608 // 3.0
)

const (
	backendKeyData       = 'K'
	authenticationX      = 'R'
	readyForQuery        = 'Z'
	rowDescription       = 'T'
	dataRow              = 'D'
	commandComplete      = 'C'
	errorResponse        = 'E'
	noticeResponse       = 'N'
	parseComplete        = '1'
	parameterDescription = 't'
	bindComplete         = '2'
	notificationResponse = 'A'
	noData               = 'n'
)

type startupMessage struct {
	options map[string]string
}

func newStartupMessage() *startupMessage {
	return &startupMessage{map[string]string{}}
}

func (self *startupMessage) Bytes() (buf []byte) {
	buf = make([]byte, 8, 128)
	binary.BigEndian.PutUint32(buf[4:8], uint32(protocolVersionNumber))
	for key, value := range self.options {
		buf = append(buf, key...)
		buf = append(buf, 0)
		buf = append(buf, value...)
		buf = append(buf, 0)
	}
	buf = append(buf, ("\000")...)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	return buf
}

type Oid int32

type FieldDescription struct {
	Name            string
	Table           Oid
	AttributeNumber int16
	DataType        Oid
	DataTypeSize    int16
	Modifier        int32
	FormatCode      int16
}

type PgError struct {
	Severity string
	Code     string
	Message  string
}

func (self PgError) Error() string {
	return self.Severity + ": " + self.Message + " (SQLSTATE " + self.Code + ")"
}

func newWriteBuf(buf []byte, t byte) *WriteBuf {
	buf = append(buf, t, 0, 0, 0, 0)
	return &WriteBuf{buf: buf, sizeIdx: 1}
}

// WrifeBuf is used build messages to send to the PostgreSQL server. It is used
// by the BinaryEncoder interface when implementing custom encoders.
type WriteBuf struct {
	buf     []byte
	sizeIdx int
}

func (wb *WriteBuf) startMsg(t byte) {
	wb.closeMsg()
	wb.buf = append(wb.buf, t, 0, 0, 0, 0)
	wb.sizeIdx = len(wb.buf) - 4
}

func (wb *WriteBuf) closeMsg() {
	binary.BigEndian.PutUint32(wb.buf[wb.sizeIdx:wb.sizeIdx+4], uint32(len(wb.buf)-wb.sizeIdx))
}

func (wb *WriteBuf) WriteByte(b byte) {
	wb.buf = append(wb.buf, b)
}

func (wb *WriteBuf) WriteCString(s string) {
	wb.buf = append(wb.buf, []byte(s)...)
	wb.buf = append(wb.buf, 0)
}

func (wb *WriteBuf) WriteInt16(n int16) {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(n))
	wb.buf = append(wb.buf, b...)
}

func (wb *WriteBuf) WriteInt32(n int32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	wb.buf = append(wb.buf, b...)
}

func (wb *WriteBuf) WriteInt64(n int64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	wb.buf = append(wb.buf, b...)
}

func (wb *WriteBuf) WriteBytes(b []byte) {
	wb.buf = append(wb.buf, b...)
}
