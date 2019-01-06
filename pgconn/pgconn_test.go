package pgconn_test

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/pgconn"
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{"Unix socket", "PGX_TEST_UNIX_SOCKET_CONN_STRING"},
		{"TCP", "PGX_TEST_TCP_CONN_STRING"},
		{"Plain password", "PGX_TEST_PLAIN_PASSWORD_CONN_STRING"},
		{"MD5 password", "PGX_TEST_MD5_PASSWORD_CONN_STRING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			connString := os.Getenv(tt.env)
			if connString == "" {
				t.Skipf("Skipping due to missing environment variable %v", tt.env)
			}

			conn, err := pgconn.Connect(context.Background(), connString)
			require.Nil(t, err)

			closeConn(t, conn)
		})
	}
}

// TestConnectTLS is separate from other connect tests because it has an additional test to ensure it really is a secure
// connection.
func TestConnectTLS(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TLS_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TLS_CONN_STRING")
	}

	conn, err := pgconn.Connect(context.Background(), connString)
	require.Nil(t, err)

	if _, ok := conn.Conn().(*tls.Conn); !ok {
		t.Error("not a TLS connection")
	}

	closeConn(t, conn)
}

func TestConnectInvalidUser(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TCP_CONN_STRING")
	}

	config, err := pgconn.ParseConfig(connString)
	require.Nil(t, err)

	config.User = "pgxinvalidusertest"

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err == nil {
		conn.Close(context.Background())
		t.Fatal("expected err but got none")
	}
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		t.Fatalf("Expected to receive a PgError, instead received: %v", err)
	}
	if pgErr.Code != "28000" && pgErr.Code != "28P01" {
		t.Fatalf("Expected to receive a PgError with code 28000 or 28P01, instead received: %v", pgErr)
	}
}

func TestConnectWithConnectionRefused(t *testing.T) {
	t.Parallel()

	// Presumably nothing is listening on 127.0.0.1:1
	conn, err := pgconn.Connect(context.Background(), "host=127.0.0.1 port=1")
	if err == nil {
		conn.Close(context.Background())
		t.Fatal("Expected error establishing connection to bad port")
	}
}

func TestConnectCustomDialer(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	dialed := false
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialed = true
		return net.Dial(network, address)
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, err)
	require.True(t, dialed)
	closeConn(t, conn)
}

func TestConnectWithRuntimeParams(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	config.RuntimeParams = map[string]string{
		"application_name": "pgxtest",
		"search_path":      "myschema",
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, err)
	defer closeConn(t, conn)

	result := conn.ExecParams(context.Background(), "show application_name", nil, nil, nil, nil).Read()
	require.Nil(t, result.Err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "pgxtest", string(result.Rows[0][0]))

	result = conn.ExecParams(context.Background(), "show search_path", nil, nil, nil, nil).Read()
	require.Nil(t, result.Err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "myschema", string(result.Rows[0][0]))
}

func TestConnectWithFallback(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	// Prepend current primary config to fallbacks
	config.Fallbacks = append([]*pgconn.FallbackConfig{
		&pgconn.FallbackConfig{
			Host:      config.Host,
			Port:      config.Port,
			TLSConfig: config.TLSConfig,
		},
	}, config.Fallbacks...)

	// Make primary config bad
	config.Host = "localhost"
	config.Port = 1 // presumably nothing listening here

	// Prepend bad first fallback
	config.Fallbacks = append([]*pgconn.FallbackConfig{
		&pgconn.FallbackConfig{
			Host:      "localhost",
			Port:      1,
			TLSConfig: config.TLSConfig,
		},
	}, config.Fallbacks...)

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, err)
	closeConn(t, conn)
}

func TestConnectWithAfterConnectFunc(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	dialCount := 0
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialCount += 1
		return net.Dial(network, address)
	}

	acceptConnCount := 0
	config.AfterConnectFunc = func(ctx context.Context, conn *pgconn.PgConn) error {
		acceptConnCount += 1
		if acceptConnCount < 2 {
			return errors.New("reject first conn")
		}
		return nil
	}

	// Append current primary config to fallbacks
	config.Fallbacks = append(config.Fallbacks, &pgconn.FallbackConfig{
		Host:      config.Host,
		Port:      config.Port,
		TLSConfig: config.TLSConfig,
	})

	// Repeat fallbacks
	config.Fallbacks = append(config.Fallbacks, config.Fallbacks...)

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, err)
	closeConn(t, conn)

	assert.True(t, dialCount > 1)
	assert.True(t, acceptConnCount > 1)
}

func TestConnectWithAfterConnectTargetSessionAttrsReadWrite(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	config.AfterConnectFunc = pgconn.AfterConnectTargetSessionAttrsReadWrite
	config.RuntimeParams["default_transaction_read_only"] = "on"

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if !assert.NotNil(t, err) {
		conn.Close(context.Background())
	}
}

func TestConnExec(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	results, err := pgConn.Exec(context.Background(), "select 'Hello, world'").ReadAll()
	assert.Nil(t, err)

	assert.Len(t, results, 1)
	assert.Nil(t, results[0].Err)
	assert.Equal(t, "SELECT 1", string(results[0].CommandTag))
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "Hello, world", string(results[0].Rows[0][0]))

	ensureConnValid(t, pgConn)
}

func TestConnExecEmpty(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	multiResult := pgConn.Exec(context.Background(), ";")

	resultCount := 0
	for multiResult.NextResult() {
		resultCount += 1
		multiResult.ResultReader().Close()
	}
	assert.Equal(t, 0, resultCount)
	err = multiResult.Close()
	assert.Nil(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecMultipleQueries(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	results, err := pgConn.Exec(context.Background(), "select 'Hello, world'; select 1").ReadAll()
	assert.Nil(t, err)

	assert.Len(t, results, 2)

	assert.Nil(t, results[0].Err)
	assert.Equal(t, "SELECT 1", string(results[0].CommandTag))
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "Hello, world", string(results[0].Rows[0][0]))

	assert.Nil(t, results[1].Err)
	assert.Equal(t, "SELECT 1", string(results[1].CommandTag))
	assert.Len(t, results[1].Rows, 1)
	assert.Equal(t, "1", string(results[1].Rows[0][0]))

	ensureConnValid(t, pgConn)
}

func TestConnExecMultipleQueriesError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	results, err := pgConn.Exec(context.Background(), "select 1; select 1/0; select 1").ReadAll()
	require.NotNil(t, err)
	if pgErr, ok := err.(*pgconn.PgError); ok {
		assert.Equal(t, "22012", pgErr.Code)
	} else {
		t.Errorf("unexpected error: %v", err)
	}

	assert.Len(t, results, 1)
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "1", string(results[0].Rows[0][0]))

	ensureConnValid(t, pgConn)
}

func TestConnExecContextCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	multiResult := pgConn.Exec(ctx, "select 'Hello, world', pg_sleep(1)")

	for multiResult.NextResult() {
	}
	err = multiResult.Close()
	assert.Equal(t, context.DeadlineExceeded, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	result := pgConn.ExecParams(context.Background(), "select $1::text", [][]byte{[]byte("Hello, world")}, nil, nil, nil)
	rowCount := 0
	for result.NextRow() {
		rowCount += 1
		assert.Equal(t, "Hello, world", string(result.Values()[0]))
	}
	assert.Equal(t, 1, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, "SELECT 1", string(commandTag))
	assert.Nil(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecParamsCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := pgConn.ExecParams(ctx, "select current_database(), pg_sleep(1)", nil, nil, nil, nil)
	rowCount := 0
	for result.NextRow() {
		rowCount += 1
	}
	assert.Equal(t, 0, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, pgconn.CommandTag(""), commandTag)
	assert.Equal(t, context.DeadlineExceeded, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecPrepared(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	psd, err := pgConn.Prepare(context.Background(), "ps1", "select $1::text", nil)
	require.Nil(t, err)
	require.NotNil(t, psd)
	assert.Len(t, psd.ParamOIDs, 1)
	assert.Len(t, psd.Fields, 1)

	result := pgConn.ExecPrepared(context.Background(), "ps1", [][]byte{[]byte("Hello, world")}, nil, nil)
	rowCount := 0
	for result.NextRow() {
		rowCount += 1
		assert.Equal(t, "Hello, world", string(result.Values()[0]))
	}
	assert.Equal(t, 1, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, "SELECT 1", string(commandTag))
	assert.Nil(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecPreparedCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Prepare(context.Background(), "ps1", "select current_database(), pg_sleep(1)", nil)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := pgConn.ExecPrepared(ctx, "ps1", nil, nil, nil)
	rowCount := 0
	for result.NextRow() {
		rowCount += 1
	}
	assert.Equal(t, 0, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, pgconn.CommandTag(""), commandTag)
	assert.Equal(t, context.DeadlineExceeded, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecBatch(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Prepare(context.Background(), "ps1", "select $1::text", nil)
	require.Nil(t, err)

	batch := &pgconn.Batch{}

	batch.ExecParams("select $1::text", [][]byte{[]byte("ExecParams 1")}, nil, nil, nil)
	batch.ExecPrepared("ps1", [][]byte{[]byte("ExecPrepared 1")}, nil, nil)
	batch.ExecParams("select $1::text", [][]byte{[]byte("ExecParams 2")}, nil, nil, nil)
	results, err := pgConn.ExecBatch(context.Background(), batch).ReadAll()
	require.Nil(t, err)
	require.Len(t, results, 3)

	require.Len(t, results[0].Rows, 1)
	require.Equal(t, "ExecParams 1", string(results[0].Rows[0][0]))
	assert.Equal(t, "SELECT 1", string(results[0].CommandTag))

	require.Len(t, results[1].Rows, 1)
	require.Equal(t, "ExecPrepared 1", string(results[1].Rows[0][0]))
	assert.Equal(t, "SELECT 1", string(results[1].CommandTag))

	require.Len(t, results[2].Rows, 1)
	require.Equal(t, "ExecParams 2", string(results[2].Rows[0][0]))
	assert.Equal(t, "SELECT 1", string(results[2].CommandTag))
}

func TestCommandTag(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		commandTag   pgconn.CommandTag
		rowsAffected int64
	}{
		{commandTag: pgconn.CommandTag("INSERT 0 5"), rowsAffected: 5},
		{commandTag: pgconn.CommandTag("UPDATE 0"), rowsAffected: 0},
		{commandTag: pgconn.CommandTag("UPDATE 1"), rowsAffected: 1},
		{commandTag: pgconn.CommandTag("DELETE 0"), rowsAffected: 0},
		{commandTag: pgconn.CommandTag("DELETE 1"), rowsAffected: 1},
		{commandTag: pgconn.CommandTag("CREATE TABLE"), rowsAffected: 0},
		{commandTag: pgconn.CommandTag("ALTER TABLE"), rowsAffected: 0},
		{commandTag: pgconn.CommandTag("DROP TABLE"), rowsAffected: 0},
	}

	for i, tt := range tests {
		actual := tt.commandTag.RowsAffected()
		assert.Equalf(t, tt.rowsAffected, actual, "%d. %v", i, tt.commandTag)
	}
}

func TestConnOnNotice(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	var msg string
	config.OnNotice = func(c *pgconn.PgConn, notice *pgconn.Notice) {
		msg = notice.Message
	}

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	multiResult := pgConn.Exec(context.Background(), `do $$
begin
  raise notice 'hello, world';
end$$;`)
	err = multiResult.Close()
	require.Nil(t, err)
	assert.Equal(t, "hello, world", msg)

	ensureConnValid(t, pgConn)
}
