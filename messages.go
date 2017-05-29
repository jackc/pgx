package pgx

import (
	"github.com/jackc/pgx/pgtype"
)

const (
	copyData = 'd'
	copyFail = 'f'
	copyDone = 'c'
)

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
