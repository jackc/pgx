package pgx

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// ErrPostgresTimeout occurs when an error was caused by a timeout. Specifically, it is true if err is or was caused
// by a context.Canceled, context.DeadlineExceeded or an implementer of net.Error where Timeout() is true.
type ErrPostgresTimeout struct {
	Err error
}

func (e *ErrPostgresTimeout) Error() string { return fmt.Sprint("postgres timeout: " + e.Err.Error()) }

func (e *ErrPostgresTimeout) Unwrap() error { return e.Err }

// wrapErrIfTimeout wraps an error if it was caused by a timeout. Otherwise, the passed error is returned as-is.
func wrapErrIfTimeout(err error) error {
	var netErr net.Error
	if (errors.As(err, &netErr) && netErr.Timeout()) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return &ErrPostgresTimeout{Err: err}
	}
	return err
}
