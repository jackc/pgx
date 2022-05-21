package pgproto3_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestLibpqMessageTracer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_CONN_STRING"))
	require.NoError(t, err)

	traceOutput := &bytes.Buffer{}

	config.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		f := pgproto3.NewFrontend(r, w)
		f.MessageTracer = &pgproto3.LibpqMessageTracer{
			Writer:             traceOutput,
			SuppressTimestamps: true,
			RegressMode:        true,
		}
		return f
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	result := conn.ExecParams(ctx, "select n from generate_series(1,5) n", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)

	expected := `F	StartupMessage
B	AuthenticationOk
B	ParameterStatus	 "application_name" ""
B	ParameterStatus	 "client_encoding" "UTF8"
B	ParameterStatus	 "DateStyle" "ISO, MDY"
B	ParameterStatus	 "default_transaction_read_only" "off"
B	ParameterStatus	 "in_hot_standby" "off"
B	ParameterStatus	 "integer_datetimes" "on"
B	ParameterStatus	 "IntervalStyle" "postgres"
B	ParameterStatus	 "is_superuser" "on"
B	ParameterStatus	 "server_encoding" "UTF8"
B	ParameterStatus	 "server_version" "14.3"
B	ParameterStatus	 "session_authorization" "jack"
B	ParameterStatus	 "standard_conforming_strings" "on"
B	ParameterStatus	 "TimeZone" "America/Chicago"
B	BackendKeyData	 NNNN NNNN
B	ReadyForQuery	 I
F	Parse	 "" "select n from generate_series(1,5) n" 0
F	Bind	 "" "" 0 0 0
F	Describe	 P ""
F	Execute	 "" 0
F	Sync
B	ParseComplete
B	BindComplete
B	RowDescription	 1 "n" 0 0 23 4 -1 0
B	DataRow	 1 1 '1'
B	DataRow	 1 1 '2'
B	DataRow	 1 1 '3'
B	DataRow	 1 1 '4'
B	DataRow	 1 1 '5'
B	CommandComplete	 "SELECT 5"
B	ReadyForQuery	 I
`

	require.Equal(t, expected, traceOutput.String())
}
