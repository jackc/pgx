package pgx

import (
	"encoding/binary"

	"github.com/jackc/pgx/pgtype"
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
	emptyQueryResponse   = 'I'
	noData               = 'n'
	closeComplete        = '3'
	flush                = 'H'
	copyInResponse       = 'G'
	copyData             = 'd'
	copyFail             = 'f'
	copyDone             = 'c'
)

type startupMessage struct {
	options map[string]string
}

func newStartupMessage() *startupMessage {
	return &startupMessage{map[string]string{}}
}

func (s *startupMessage) Bytes() (buf []byte) {
	buf = make([]byte, 8, 128)
	binary.BigEndian.PutUint32(buf[4:8], uint32(protocolVersionNumber))
	for key, value := range s.options {
		buf = append(buf, key...)
		buf = append(buf, 0)
		buf = append(buf, value...)
		buf = append(buf, 0)
	}
	buf = append(buf, ("\000")...)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(buf)))
	return buf
}

type FieldDescription struct {
	Name            string
	Table           pgtype.Oid
	AttributeNumber uint16
	DataType        pgtype.Oid
	DataTypeSize    int16
	DataTypeName    string
	Modifier        uint32
	FormatCode      int16
}

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/9.3/static/protocol-error-fields.html for
// detailed field description.
type PgError struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	ConstraintName   string
	File             string
	Line             int32
	Routine          string
}

func (pe PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}
