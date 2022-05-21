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

func TestTrace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_CONN_STRING"))
	require.NoError(t, err)

	traceOutput := &bytes.Buffer{}

	config.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		f := pgproto3.NewFrontend(r, w)
		f.Trace(traceOutput, pgproto3.TracerOptions{
			SuppressTimestamps: true,
			RegressMode:        true,
		})
		return f
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	result := conn.ExecParams(ctx, "select n from generate_series(1,5) n", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)

	expected := `F	StartupMessage	37
B	AuthenticationOk	9
B	ParameterStatus	23	 "application_name" ""
B	ParameterStatus	26	 "client_encoding" "UTF8"
B	ParameterStatus	24	 "DateStyle" "ISO, MDY"
B	ParameterStatus	39	 "default_transaction_read_only" "off"
B	ParameterStatus	24	 "in_hot_standby" "off"
B	ParameterStatus	26	 "integer_datetimes" "on"
B	ParameterStatus	28	 "IntervalStyle" "postgres"
B	ParameterStatus	21	 "is_superuser" "on"
B	ParameterStatus	26	 "server_encoding" "UTF8"
B	ParameterStatus	25	 "server_version" "14.3"
B	ParameterStatus	32	 "session_authorization" "jack"
B	ParameterStatus	36	 "standard_conforming_strings" "on"
B	ParameterStatus	30	 "TimeZone" "America/Chicago"
B	BackendKeyData	13	 NNNN NNNN
B	ReadyForQuery	6	 I
F	Parse	45	 "" "select n from generate_series(1,5) n" 0
F	Bind	13	 "" "" 0 0 0
F	Describe	7	 P ""
F	Execute	10	 "" 0
F	Sync	5
B	ParseComplete	5
B	BindComplete	5
B	RowDescription	27	 1 "n" 0 0 23 4 -1 0
B	DataRow	12	 1 1 '1'
B	DataRow	12	 1 1 '2'
B	DataRow	12	 1 1 '3'
B	DataRow	12	 1 1 '4'
B	DataRow	12	 1 1 '5'
B	CommandComplete	14	 "SELECT 5"
B	ReadyForQuery	6	 I
`

	require.Equal(t, expected, traceOutput.String())
}
