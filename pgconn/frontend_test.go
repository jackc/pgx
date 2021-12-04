package pgconn_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// frontendWrapper allows to hijack a regular frontend, and inject a specific response
type frontendWrapper struct {
	front pgconn.Frontend

	msg pgproto3.BackendMessage
}

// frontendWrapper implements the pgconn.Frontend interface
var _ pgconn.Frontend = (*frontendWrapper)(nil)

func (f *frontendWrapper) Receive() (pgproto3.BackendMessage, error) {
	if f.msg != nil {
		return f.msg, nil
	}

	return f.front.Receive()
}

func TestFrontendFatalErrExec(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_CONN_STRING"))
	require.NoError(t, err)

	buildFrontend := config.BuildFrontend
	var front *frontendWrapper

	config.BuildFrontend = func(r io.Reader, w io.Writer) pgconn.Frontend {
		wrapped := buildFrontend(r, w)
		front = &frontendWrapper{wrapped, nil}

		return front
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.NotNil(t, front)

	// set frontend to return a "FATAL" message on next call
	front.msg = &pgproto3.ErrorResponse{Severity: "FATAL", Message: "unit testing fatal error"}

	_, err = conn.Exec(context.Background(), "SELECT 1").ReadAll()
	assert.Error(t, err)

	err = conn.Close(context.Background())
	assert.NoError(t, err)

	select {
	case <-conn.CleanupDone():
		t.Log("ok, CleanupDone() is not blocking")

	default:
		assert.Fail(t, "connection closed but CleanupDone() still blocking")
	}
}
