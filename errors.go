package pgconn

import (
	"context"
	"net"

	errors "golang.org/x/xerrors"
)

// ErrTLSRefused occurs when the connection attempt requires TLS and the
// PostgreSQL server refuses to use TLS
var ErrTLSRefused = errors.New("server refused TLS connection")

// ErrConnBusy occurs when the connection is busy (for example, in the middle of reading query results) and another
// action is attempted.
var ErrConnBusy = errors.New("conn is busy")

// ErrNoBytesSent is used to annotate an error that occurred without sending any bytes to the server. This can be used
// to implement safe retry logic. ErrNoBytesSent will never occur alone. It will always be wrapped by another error.
var ErrNoBytesSent = errors.New("no bytes sent to server")

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/11/static/protocol-error-fields.html for
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

func (pe *PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}

// linkedError connects two errors as if err wrapped next.
type linkedError struct {
	err  error
	next error
}

func (le *linkedError) Error() string {
	return le.err.Error()
}

func (le *linkedError) Is(target error) bool {
	return errors.Is(le.err, target)
}

func (le *linkedError) As(target interface{}) bool {
	return errors.As(le.err, target)
}

func (le *linkedError) Unwrap() error {
	return le.next
}

// preferContextOverNetTimeoutError returns ctx.Err() if ctx.Err() is present and err is a net.Error with Timeout() ==
// true. Otherwise returns err.
func preferContextOverNetTimeoutError(ctx context.Context, err error) error {
	if err, ok := err.(net.Error); ok && err.Timeout() && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// linkErrors connects outer and inner as if the the fully unwrapped outer wrapped inner. If either outer or inner is nil then the other is returned.
func linkErrors(outer, inner error) error {
	if outer == nil {
		return inner
	}
	if inner == nil {
		return outer
	}
	return &linkedError{err: outer, next: inner}
}
