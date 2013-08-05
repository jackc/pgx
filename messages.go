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
