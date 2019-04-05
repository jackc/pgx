package pgconn_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgconn"
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
			require.NoError(t, err)

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
	require.NoError(t, err)

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
	require.NoError(t, err)

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
	require.NoError(t, err)

	dialed := false
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialed = true
		return net.Dial(network, address)
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	require.True(t, dialed)
	closeConn(t, conn)
}

func TestConnectWithRuntimeParams(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.RuntimeParams = map[string]string{
		"application_name": "pgxtest",
		"search_path":      "myschema",
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
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
	require.NoError(t, err)

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
	require.NoError(t, err)
	closeConn(t, conn)
}

func TestConnectWithAfterConnectFunc(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

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
	require.NoError(t, err)
	closeConn(t, conn)

	assert.True(t, dialCount > 1)
	assert.True(t, acceptConnCount > 1)
}

func TestConnectWithAfterConnectTargetSessionAttrsReadWrite(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.AfterConnectFunc = pgconn.AfterConnectTargetSessionAttrsReadWrite
	config.RuntimeParams["default_transaction_read_only"] = "on"

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if !assert.NotNil(t, err) {
		conn.Close(context.Background())
	}
}

func TestConnPrepareSyntaxError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	psd, err := pgConn.Prepare(context.Background(), "ps1", "SYNTAX ERROR", nil)
	require.Nil(t, psd)
	require.NotNil(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExec(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	results, err := pgConn.Exec(context.Background(), "select 'Hello, world'").ReadAll()
	assert.NoError(t, err)

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
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	multiResult := pgConn.Exec(context.Background(), ";")

	resultCount := 0
	for multiResult.NextResult() {
		resultCount += 1
		multiResult.ResultReader().Close()
	}
	assert.Equal(t, 0, resultCount)
	err = multiResult.Close()
	assert.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecMultipleQueries(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	results, err := pgConn.Exec(context.Background(), "select 'Hello, world'; select 1").ReadAll()
	assert.NoError(t, err)

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
	require.NoError(t, err)
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
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	multiResult := pgConn.Exec(ctx, "select 'Hello, world', pg_sleep(1)")

	for multiResult.NextResult() {
	}
	err = multiResult.Close()
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.False(t, pgConn.IsAlive())
}

func TestConnExecParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
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
	assert.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecParamsCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
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

	assert.False(t, pgConn.IsAlive())
}

func TestConnExecPrepared(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	psd, err := pgConn.Prepare(context.Background(), "ps1", "select $1::text", nil)
	require.NoError(t, err)
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
	assert.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecPreparedCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Prepare(context.Background(), "ps1", "select current_database(), pg_sleep(1)", nil)
	require.NoError(t, err)

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
	assert.False(t, pgConn.IsAlive())
}

func TestConnExecBatch(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Prepare(context.Background(), "ps1", "select $1::text", nil)
	require.NoError(t, err)

	batch := &pgconn.Batch{}

	batch.ExecParams("select $1::text", [][]byte{[]byte("ExecParams 1")}, nil, nil, nil)
	batch.ExecPrepared("ps1", [][]byte{[]byte("ExecPrepared 1")}, nil, nil)
	batch.ExecParams("select $1::text", [][]byte{[]byte("ExecParams 2")}, nil, nil, nil)
	results, err := pgConn.ExecBatch(context.Background(), batch).ReadAll()
	require.NoError(t, err)
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

func TestConnLocking(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	mrr := pgConn.Exec(context.Background(), "select 'Hello, world'")
	results, err := pgConn.Exec(context.Background(), "select 'Hello, world'").ReadAll()
	assert.Error(t, err)
	assert.Equal(t, "connection busy", err.Error())

	results, err = mrr.ReadAll()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Nil(t, results[0].Err)
	assert.Equal(t, "SELECT 1", string(results[0].CommandTag))
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "Hello, world", string(results[0].Rows[0][0]))

	ensureConnValid(t, pgConn)
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
	require.NoError(t, err)

	var msg string
	config.OnNotice = func(c *pgconn.PgConn, notice *pgconn.Notice) {
		msg = notice.Message
	}

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	multiResult := pgConn.Exec(context.Background(), `do $$
begin
  raise notice 'hello, world';
end$$;`)
	err = multiResult.Close()
	require.NoError(t, err)
	assert.Equal(t, "hello, world", msg)

	ensureConnValid(t, pgConn)
}

func TestConnOnNotification(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	var msg string
	config.OnNotification = func(c *pgconn.PgConn, n *pgconn.Notification) {
		msg = n.Payload
	}

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), "listen foo").ReadAll()
	require.NoError(t, err)

	notifier, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, notifier)
	_, err = notifier.Exec(context.Background(), "notify foo, 'bar'").ReadAll()
	require.NoError(t, err)

	_, err = pgConn.Exec(context.Background(), "select 1").ReadAll()
	require.NoError(t, err)

	assert.Equal(t, "bar", msg)

	ensureConnValid(t, pgConn)
}

func TestConnWaitForNotification(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	var msg string
	config.OnNotification = func(c *pgconn.PgConn, n *pgconn.Notification) {
		msg = n.Payload
	}

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), "listen foo").ReadAll()
	require.NoError(t, err)

	notifier, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, notifier)
	_, err = notifier.Exec(context.Background(), "notify foo, 'bar'").ReadAll()
	require.NoError(t, err)

	err = pgConn.WaitForNotification(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "bar", msg)

	ensureConnValid(t, pgConn)
}

func TestConnWaitForNotificationTimeout(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err = pgConn.WaitForNotification(ctx)
	cancel()
	require.Equal(t, context.DeadlineExceeded, err)

	ensureConnValid(t, pgConn)
}

func TestConnCopyToSmall(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g json
	)`).ReadAll()
	require.NoError(t, err)

	_, err = pgConn.Exec(context.Background(), `insert into foo values (0, 1, 2, 'abc', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}')`).ReadAll()
	require.NoError(t, err)

	_, err = pgConn.Exec(context.Background(), `insert into foo values (null, null, null, null, null, null, null)`).ReadAll()
	require.NoError(t, err)

	inputBytes := []byte("0\t1\t2\tabc\tefg\t2000-01-01\t{\"abc\":\"def\",\"foo\":\"bar\"}\n" +
		"\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\n")

	outputWriter := bytes.NewBuffer(make([]byte, 0, len(inputBytes)))

	res, err := pgConn.CopyTo(context.Background(), outputWriter, "copy foo to stdout")
	require.NoError(t, err)

	assert.Equal(t, int64(2), res.RowsAffected())
	assert.Equal(t, inputBytes, outputWriter.Bytes())

	ensureConnValid(t, pgConn)
}

func TestConnCopyToLarge(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), `create temporary table foo(
		a int2,
		b int4,
		c int8,
		d varchar,
		e text,
		f date,
		g json,
		h bytea
	)`).ReadAll()
	require.NoError(t, err)

	inputBytes := make([]byte, 0)

	for i := 0; i < 1000; i++ {
		_, err = pgConn.Exec(context.Background(), `insert into foo values (0, 1, 2, 'abc', 'efg', '2000-01-01', '{"abc":"def","foo":"bar"}', 'oooo')`).ReadAll()
		require.NoError(t, err)
		inputBytes = append(inputBytes, "0\t1\t2\tabc\tefg\t2000-01-01\t{\"abc\":\"def\",\"foo\":\"bar\"}\t\\\\x6f6f6f6f\n"...)
	}

	outputWriter := bytes.NewBuffer(make([]byte, 0, len(inputBytes)))

	res, err := pgConn.CopyTo(context.Background(), outputWriter, "copy foo to stdout")
	require.NoError(t, err)

	assert.Equal(t, int64(1000), res.RowsAffected())
	assert.Equal(t, inputBytes, outputWriter.Bytes())

	ensureConnValid(t, pgConn)
}

func TestConnCopyToQueryError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	outputWriter := bytes.NewBuffer(make([]byte, 0))

	res, err := pgConn.CopyTo(context.Background(), outputWriter, "cropy foo to stdout")
	require.Error(t, err)
	assert.IsType(t, &pgconn.PgError{}, err)
	assert.Equal(t, int64(0), res.RowsAffected())

	ensureConnValid(t, pgConn)
}

func TestConnCopyToCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	outputWriter := &bytes.Buffer{}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := pgConn.CopyTo(ctx, outputWriter, "copy (select *, pg_sleep(0.01) from generate_series(1,1000)) to stdout")
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Equal(t, pgconn.CommandTag(""), res)

	assert.False(t, pgConn.IsAlive())
}

func TestConnCopyFrom(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), `create temporary table foo(
		a int4,
		b varchar
	)`).ReadAll()
	require.NoError(t, err)

	srcBuf := &bytes.Buffer{}

	inputRows := [][][]byte{}
	for i := 0; i < 1000; i++ {
		a := strconv.Itoa(i)
		b := "foo " + a + " bar"
		inputRows = append(inputRows, [][]byte{[]byte(a), []byte(b)})
		_, err = srcBuf.Write([]byte(fmt.Sprintf("%s,\"%s\"\n", a, b)))
		require.NoError(t, err)
	}

	ct, err := pgConn.CopyFrom(context.Background(), srcBuf, "COPY foo FROM STDIN WITH (FORMAT csv)")
	require.NoError(t, err)
	assert.Equal(t, int64(len(inputRows)), ct.RowsAffected())

	result := pgConn.ExecParams(context.Background(), "select * from foo", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)

	assert.Equal(t, inputRows, result.Rows)

	ensureConnValid(t, pgConn)
}

func TestConnCopyFromCanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), `create temporary table foo(
		a int4,
		b varchar
	)`).ReadAll()
	require.NoError(t, err)

	r, w := io.Pipe()
	go func() {
		for i := 0; i < 1000000; i++ {
			a := strconv.Itoa(i)
			b := "foo " + a + " bar"
			_, err := w.Write([]byte(fmt.Sprintf("%s,\"%s\"\n", a, b)))
			if err != nil {
				return
			}
			time.Sleep(time.Microsecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	ct, err := pgConn.CopyFrom(ctx, r, "COPY foo FROM STDIN WITH (FORMAT csv)")
	cancel()
	assert.Equal(t, int64(0), ct.RowsAffected())
	require.Equal(t, context.DeadlineExceeded, err)

	assert.False(t, pgConn.IsAlive())
}

func TestConnCopyFromGzipReader(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), `create temporary table foo(
		a int4,
		b varchar
	)`).ReadAll()
	require.NoError(t, err)

	f, err := ioutil.TempFile("", "*")
	require.NoError(t, err)

	gw := gzip.NewWriter(f)

	inputRows := [][][]byte{}
	for i := 0; i < 1000; i++ {
		a := strconv.Itoa(i)
		b := "foo " + a + " bar"
		inputRows = append(inputRows, [][]byte{[]byte(a), []byte(b)})
		_, err = gw.Write([]byte(fmt.Sprintf("%s,\"%s\"\n", a, b)))
		require.NoError(t, err)
	}

	err = gw.Close()
	require.NoError(t, err)

	_, err = f.Seek(0, 0)
	require.NoError(t, err)

	gr, err := gzip.NewReader(f)
	require.NoError(t, err)

	ct, err := pgConn.CopyFrom(context.Background(), gr, "COPY foo FROM STDIN WITH (FORMAT csv)")
	require.NoError(t, err)
	assert.Equal(t, int64(len(inputRows)), ct.RowsAffected())

	err = gr.Close()
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	err = os.Remove(f.Name())
	require.NoError(t, err)

	result := pgConn.ExecParams(context.Background(), "select * from foo", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)

	assert.Equal(t, inputRows, result.Rows)

	ensureConnValid(t, pgConn)
}

func TestConnCopyFromQuerySyntaxError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), `create temporary table foo(
		a int4,
		b varchar
	)`).ReadAll()
	require.NoError(t, err)

	srcBuf := &bytes.Buffer{}

	res, err := pgConn.CopyFrom(context.Background(), srcBuf, "cropy foo to stdout")
	require.Error(t, err)
	assert.IsType(t, &pgconn.PgError{}, err)
	assert.Equal(t, int64(0), res.RowsAffected())

	ensureConnValid(t, pgConn)
}

func TestConnCopyFromQueryNoTableError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	srcBuf := &bytes.Buffer{}

	res, err := pgConn.CopyFrom(context.Background(), srcBuf, "cropy foo to stdout")
	require.Error(t, err)
	assert.IsType(t, &pgconn.PgError{}, err)
	assert.Equal(t, int64(0), res.RowsAffected())

	ensureConnValid(t, pgConn)
}

func TestConnEscapeString(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	tests := []struct {
		in  string
		out string
	}{
		{in: "", out: ""},
		{in: "42", out: "42"},
		{in: "'", out: "''"},
		{in: "hi'there", out: "hi''there"},
		{in: "'hi there'", out: "''hi there''"},
	}

	for i, tt := range tests {
		value, err := pgConn.EscapeString(tt.in)
		if assert.NoErrorf(t, err, "%d.", i) {
			assert.Equalf(t, tt.out, value, "%d.", i)
		}
	}

	ensureConnValid(t, pgConn)
}

func TestConnCancelRequest(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	multiResult := pgConn.Exec(context.Background(), "select 'Hello, world', pg_sleep(5)")

	err = pgConn.CancelRequest(context.Background())
	require.NoError(t, err)

	for multiResult.NextResult() {
	}
	err = multiResult.Close()

	require.IsType(t, &pgconn.PgError{}, err)
	require.Equal(t, "57014", err.(*pgconn.PgError).Code)

	ensureConnValid(t, pgConn)
}

func Example() {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		log.Fatalln(err)
	}
	defer pgConn.Close(context.Background())

	result := pgConn.ExecParams(context.Background(), "select generate_series(1,3)", nil, nil, nil, nil).Read()
	if result.Err != nil {
		log.Fatalln(result.Err)
	}

	for _, row := range result.Rows {
		fmt.Println(string(row[0]))
	}

	fmt.Println(result.CommandTag)
	// Output:
	// 1
	// 2
	// 3
	// SELECT 3
}
