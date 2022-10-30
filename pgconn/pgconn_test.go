package pgconn_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/pgmock"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
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
		{"SCRAM password", "PGX_TEST_SCRAM_PASSWORD_CONN_STRING"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
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

func TestConnectWithOptions(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{"Unix socket", "PGX_TEST_UNIX_SOCKET_CONN_STRING"},
		{"TCP", "PGX_TEST_TCP_CONN_STRING"},
		{"Plain password", "PGX_TEST_PLAIN_PASSWORD_CONN_STRING"},
		{"MD5 password", "PGX_TEST_MD5_PASSWORD_CONN_STRING"},
		{"SCRAM password", "PGX_TEST_SCRAM_PASSWORD_CONN_STRING"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			connString := os.Getenv(tt.env)
			if connString == "" {
				t.Skipf("Skipping due to missing environment variable %v", tt.env)
			}
			var sslOptions pgconn.ParseConfigOptions
			sslOptions.GetSSLPassword = GetSSLPassword
			conn, err := pgconn.ConnectWithOptions(context.Background(), connString, sslOptions)
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

	result := conn.ExecParams(context.Background(), `select ssl from pg_stat_ssl where pg_backend_pid() = pid;`, nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	require.Equalf(t, "t", string(result.Rows[0][0]), "not a TLS connection")

	closeConn(t, conn)
}

func TestConnectTLSPasswordProtectedClientCertWithSSLPassword(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TLS_CLIENT_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TLS_CLIENT_CONN_STRING")
	}
	if os.Getenv("PGX_SSL_PASSWORD") == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_SSL_PASSWORD")
	}

	connString += " sslpassword=" + os.Getenv("PGX_SSL_PASSWORD")

	conn, err := pgconn.Connect(context.Background(), connString)
	require.NoError(t, err)

	result := conn.ExecParams(context.Background(), `select ssl from pg_stat_ssl where pg_backend_pid() = pid;`, nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	require.Equalf(t, "t", string(result.Rows[0][0]), "not a TLS connection")

	closeConn(t, conn)
}

func TestConnectTLSPasswordProtectedClientCertWithGetSSLPasswordConfigOption(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TLS_CLIENT_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TLS_CLIENT_CONN_STRING")
	}
	if os.Getenv("PGX_SSL_PASSWORD") == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_SSL_PASSWORD")
	}

	var sslOptions pgconn.ParseConfigOptions
	sslOptions.GetSSLPassword = GetSSLPassword
	config, err := pgconn.ParseConfigWithOptions(connString, sslOptions)
	require.Nil(t, err)

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	result := conn.ExecParams(context.Background(), `select ssl from pg_stat_ssl where pg_backend_pid() = pid;`, nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	require.Equalf(t, "t", string(result.Rows[0][0]), "not a TLS connection")

	closeConn(t, conn)
}

type pgmockWaitStep time.Duration

func (s pgmockWaitStep) Step(*pgproto3.Backend) error {
	time.Sleep(time.Duration(s))
	return nil
}

func TestConnectTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		connect func(connStr string) error
	}{
		{
			name: "via context that times out",
			connect: func(connStr string) error {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()
				_, err := pgconn.Connect(ctx, connStr)
				return err
			},
		},
		{
			name: "via config ConnectTimeout",
			connect: func(connStr string) error {
				conf, err := pgconn.ParseConfig(connStr)
				require.NoError(t, err)
				conf.ConnectTimeout = time.Microsecond * 50
				_, err = pgconn.ConnectConfig(context.Background(), conf)
				return err
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			script := &pgmock.Script{
				Steps: []pgmock.Step{
					pgmock.ExpectAnyMessage(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{}}),
					pgmock.SendMessage(&pgproto3.AuthenticationOk{}),
					pgmockWaitStep(time.Millisecond * 500),
					pgmock.SendMessage(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0}),
					pgmock.SendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'}),
				},
			}

			ln, err := net.Listen("tcp", "127.0.0.1:")
			require.NoError(t, err)
			defer ln.Close()

			serverErrChan := make(chan error, 1)
			go func() {
				defer close(serverErrChan)

				conn, err := ln.Accept()
				if err != nil {
					serverErrChan <- err
					return
				}
				defer conn.Close()

				err = conn.SetDeadline(time.Now().Add(time.Millisecond * 450))
				if err != nil {
					serverErrChan <- err
					return
				}

				err = script.Run(pgproto3.NewBackend(conn, conn))
				if err != nil {
					serverErrChan <- err
					return
				}
			}()

			parts := strings.Split(ln.Addr().String(), ":")
			host := parts[0]
			port := parts[1]
			connStr := fmt.Sprintf("sslmode=disable host=%s port=%s", host, port)
			tooLate := time.Now().Add(time.Millisecond * 500)

			err = tt.connect(connStr)
			require.True(t, pgconn.Timeout(err), err)
			require.True(t, time.Now().Before(tooLate))
		})
	}
}

func TestConnectTimeoutStuckOnTLSHandshake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		connect func(connStr string) error
	}{
		{
			name: "via context that times out",
			connect: func(connStr string) error {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
				defer cancel()
				_, err := pgconn.Connect(ctx, connStr)
				return err
			},
		},
		{
			name: "via config ConnectTimeout",
			connect: func(connStr string) error {
				conf, err := pgconn.ParseConfig(connStr)
				require.NoError(t, err)
				conf.ConnectTimeout = time.Millisecond * 10
				_, err = pgconn.ConnectConfig(context.Background(), conf)
				return err
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ln, err := net.Listen("tcp", "127.0.0.1:")
			require.NoError(t, err)
			defer ln.Close()

			serverErrChan := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					serverErrChan <- err
					return
				}
				defer conn.Close()

				var buf []byte
				_, err = conn.Read(buf)
				if err != nil {
					serverErrChan <- err
					return
				}

				// Sleeping to hang the TLS handshake.
				time.Sleep(time.Minute)
			}()

			parts := strings.Split(ln.Addr().String(), ":")
			host := parts[0]
			port := parts[1]
			connStr := fmt.Sprintf("host=%s port=%s", host, port)

			errChan := make(chan error)
			go func() {
				err := tt.connect(connStr)
				errChan <- err
			}()

			select {
			case err = <-errChan:
				require.True(t, pgconn.Timeout(err), err)
			case err = <-serverErrChan:
				t.Fatalf("server failed with error: %s", err)
			case <-time.After(time.Millisecond * 100):
				t.Fatal("exceeded connection timeout without erroring out")
			}
		})
	}
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

	_, err = pgconn.ConnectConfig(context.Background(), config)
	require.Error(t, err)
	pgErr, ok := errors.Unwrap(err).(*pgconn.PgError)
	if !ok {
		t.Fatalf("Expected to receive a wrapped PgError, instead received: %v", err)
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

func TestConnectCustomLookup(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TCP_CONN_STRING")
	}

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	looked := false
	config.LookupFunc = func(ctx context.Context, host string) (addrs []string, err error) {
		looked = true
		return net.LookupHost(host)
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	require.True(t, looked)
	closeConn(t, conn)
}

func TestConnectCustomLookupWithPort(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TCP_CONN_STRING")
	}

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	origPort := config.Port
	// Chnage the config an invalid port so it will fail if used
	config.Port = 0

	looked := false
	config.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
		looked = true
		addrs, err := net.LookupHost(host)
		if err != nil {
			return nil, err
		}
		for i := range addrs {
			addrs[i] = net.JoinHostPort(addrs[i], strconv.FormatUint(uint64(origPort), 10))
		}
		return addrs, nil
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	require.True(t, looked)
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

func TestConnectWithValidateConnect(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	dialCount := 0
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialCount++
		return net.Dial(network, address)
	}

	acceptConnCount := 0
	config.ValidateConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		acceptConnCount++
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

func TestConnectWithValidateConnectTargetSessionAttrsReadWrite(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.ValidateConnect = pgconn.ValidateConnectTargetSessionAttrsReadWrite
	config.RuntimeParams["default_transaction_read_only"] = "on"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := pgconn.ConnectConfig(ctx, config)
	if !assert.NotNil(t, err) {
		conn.Close(ctx)
	}
}

func TestConnectWithAfterConnect(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		_, err := conn.Exec(ctx, "set search_path to foobar;").ReadAll()
		return err
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	results, err := conn.Exec(context.Background(), "show search_path;").ReadAll()
	require.NoError(t, err)
	defer closeConn(t, conn)

	assert.Equal(t, []byte("foobar"), results[0].Rows[0][0])
}

func TestConnectConfigRequiresConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &pgconn.Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { pgconn.ConnectConfig(context.Background(), config) })
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

func TestConnPrepareContextPrecanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	psd, err := pgConn.Prepare(ctx, "ps1", "select 1", nil)
	assert.Nil(t, psd)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(err))

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
	assert.Equal(t, "SELECT 1", results[0].CommandTag.String())
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
		resultCount++
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
	assert.Equal(t, "SELECT 1", results[0].CommandTag.String())
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "Hello, world", string(results[0].Rows[0][0]))

	assert.Nil(t, results[1].Err)
	assert.Equal(t, "SELECT 1", results[1].CommandTag.String())
	assert.Len(t, results[1].Rows, 1)
	assert.Equal(t, "1", string(results[1].Rows[0][0]))

	ensureConnValid(t, pgConn)
}

func TestConnExecMultipleQueriesEagerFieldDescriptions(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	mrr := pgConn.Exec(context.Background(), "select 'Hello, world' as msg; select 1 as num")

	require.True(t, mrr.NextResult())
	require.Len(t, mrr.ResultReader().FieldDescriptions(), 1)
	assert.Equal(t, "msg", mrr.ResultReader().FieldDescriptions()[0].Name)
	_, err = mrr.ResultReader().Close()
	require.NoError(t, err)

	require.True(t, mrr.NextResult())
	require.Len(t, mrr.ResultReader().FieldDescriptions(), 1)
	assert.Equal(t, "num", mrr.ResultReader().FieldDescriptions()[0].Name)
	_, err = mrr.ResultReader().Close()
	require.NoError(t, err)

	require.False(t, mrr.NextResult())

	require.NoError(t, mrr.Close())

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

	if pgConn.ParameterStatus("crdb_version") != "" {
		// CockroachDB starts the second query result set and then sends the divide by zero error.
		require.Len(t, results, 2)
		assert.Len(t, results[0].Rows, 1)
		assert.Equal(t, "1", string(results[0].Rows[0][0]))
		assert.Len(t, results[1].Rows, 0)
	} else {
		// PostgreSQL sends the divide by zero and never sends the second query result set.
		require.Len(t, results, 1)
		assert.Len(t, results[0].Rows, 1)
		assert.Equal(t, "1", string(results[0].Rows[0][0]))
	}

	ensureConnValid(t, pgConn)
}

func TestConnExecDeferredError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")
	}

	setupSQL := `create temporary table t (
		id text primary key,
		n int not null,
		unique (n) deferrable initially deferred
	);

	insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);`

	_, err = pgConn.Exec(context.Background(), setupSQL).ReadAll()
	assert.NoError(t, err)

	_, err = pgConn.Exec(context.Background(), `update t set n=n+1 where id='b' returning *`).ReadAll()
	require.NotNil(t, err)

	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23505", pgErr.Code)

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
	assert.True(t, pgconn.Timeout(err))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, pgConn.IsClosed())
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

func TestConnExecContextPrecanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pgConn.Exec(ctx, "select 'Hello, world'").ReadAll()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(err))

	ensureConnValid(t, pgConn)
}

func TestConnExecParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	result := pgConn.ExecParams(context.Background(), "select $1::text as msg", [][]byte{[]byte("Hello, world")}, nil, nil, nil)
	require.Len(t, result.FieldDescriptions(), 1)
	assert.Equal(t, "msg", result.FieldDescriptions()[0].Name)

	rowCount := 0
	for result.NextRow() {
		rowCount += 1
		assert.Equal(t, "Hello, world", string(result.Values()[0]))
	}
	assert.Equal(t, 1, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, "SELECT 1", commandTag.String())
	assert.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecParamsDeferredError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")
	}

	setupSQL := `create temporary table t (
		id text primary key,
		n int not null,
		unique (n) deferrable initially deferred
	);

	insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);`

	_, err = pgConn.Exec(context.Background(), setupSQL).ReadAll()
	assert.NoError(t, err)

	result := pgConn.ExecParams(context.Background(), `update t set n=n+1 where id='b' returning *`, nil, nil, nil, nil).Read()
	require.NotNil(t, result.Err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(result.Err, &pgErr))
	require.Equal(t, "23505", pgErr.Code)

	ensureConnValid(t, pgConn)
}

func TestConnExecParamsMaxNumberOfParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	paramCount := math.MaxUint16
	params := make([]string, 0, paramCount)
	args := make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, fmt.Sprintf("($%d::text)", i+1))
		args = append(args, []byte(strconv.Itoa(i)))
	}
	sql := "values" + strings.Join(params, ", ")

	result := pgConn.ExecParams(context.Background(), sql, args, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Len(t, result.Rows, paramCount)

	ensureConnValid(t, pgConn)
}

func TestConnExecParamsTooManyParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	paramCount := math.MaxUint16 + 1
	params := make([]string, 0, paramCount)
	args := make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, fmt.Sprintf("($%d::text)", i+1))
		args = append(args, []byte(strconv.Itoa(i)))
	}
	sql := "values" + strings.Join(params, ", ")

	result := pgConn.ExecParams(context.Background(), sql, args, nil, nil, nil).Read()
	require.Error(t, result.Err)
	require.Equal(t, "extended protocol limited to 65535 parameters", result.Err.Error())

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
	assert.Equal(t, pgconn.CommandTag{}, commandTag)
	assert.True(t, pgconn.Timeout(err))
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	assert.True(t, pgConn.IsClosed())
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

func TestConnExecParamsPrecanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := pgConn.ExecParams(ctx, "select $1::text", [][]byte{[]byte("Hello, world")}, nil, nil, nil).Read()
	require.Error(t, result.Err)
	assert.True(t, errors.Is(result.Err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(result.Err))

	ensureConnValid(t, pgConn)
}

func TestConnExecParamsEmptySQL(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	pgConn, err := pgconn.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	result := pgConn.ExecParams(ctx, "", nil, nil, nil, nil).Read()
	assert.Equal(t, pgconn.CommandTag{}, result.CommandTag)
	assert.Len(t, result.Rows, 0)
	assert.NoError(t, result.Err)

	ensureConnValid(t, pgConn)
}

// https://github.com/jackc/pgx/issues/859
func TestResultReaderValuesHaveSameCapacityAsLength(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	result := pgConn.ExecParams(context.Background(), "select $1::text as msg", [][]byte{[]byte("Hello, world")}, nil, nil, nil)
	require.Len(t, result.FieldDescriptions(), 1)
	assert.Equal(t, "msg", result.FieldDescriptions()[0].Name)

	rowCount := 0
	for result.NextRow() {
		rowCount += 1
		assert.Equal(t, "Hello, world", string(result.Values()[0]))
		assert.Equal(t, len(result.Values()[0]), cap(result.Values()[0]))
	}
	assert.Equal(t, 1, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, "SELECT 1", commandTag.String())
	assert.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecPrepared(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	psd, err := pgConn.Prepare(context.Background(), "ps1", "select $1::text as msg", nil)
	require.NoError(t, err)
	require.NotNil(t, psd)
	assert.Len(t, psd.ParamOIDs, 1)
	assert.Len(t, psd.Fields, 1)

	result := pgConn.ExecPrepared(context.Background(), "ps1", [][]byte{[]byte("Hello, world")}, nil, nil)
	require.Len(t, result.FieldDescriptions(), 1)
	assert.Equal(t, "msg", result.FieldDescriptions()[0].Name)

	rowCount := 0
	for result.NextRow() {
		rowCount += 1
		assert.Equal(t, "Hello, world", string(result.Values()[0]))
	}
	assert.Equal(t, 1, rowCount)
	commandTag, err := result.Close()
	assert.Equal(t, "SELECT 1", commandTag.String())
	assert.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestConnExecPreparedMaxNumberOfParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	paramCount := math.MaxUint16
	params := make([]string, 0, paramCount)
	args := make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, fmt.Sprintf("($%d::text)", i+1))
		args = append(args, []byte(strconv.Itoa(i)))
	}
	sql := "values" + strings.Join(params, ", ")

	psd, err := pgConn.Prepare(context.Background(), "ps1", sql, nil)
	require.NoError(t, err)
	require.NotNil(t, psd)
	assert.Len(t, psd.ParamOIDs, paramCount)
	assert.Len(t, psd.Fields, 1)

	result := pgConn.ExecPrepared(context.Background(), "ps1", args, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Len(t, result.Rows, paramCount)

	ensureConnValid(t, pgConn)
}

func TestConnExecPreparedTooManyParams(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	paramCount := math.MaxUint16 + 1
	params := make([]string, 0, paramCount)
	args := make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, fmt.Sprintf("($%d::text)", i+1))
		args = append(args, []byte(strconv.Itoa(i)))
	}
	sql := "values" + strings.Join(params, ", ")

	psd, err := pgConn.Prepare(context.Background(), "ps1", sql, nil)
	if pgConn.ParameterStatus("crdb_version") != "" {
		// CockroachDB rejects preparing a statement with more than 65535 parameters.
		require.EqualError(t, err, "ERROR: more than 65535 arguments to prepared statement: 65536 (SQLSTATE 08P01)")
	} else {
		// PostgreSQL accepts preparing a statement with more than 65535 parameters and only fails when executing it through the extended protocol.
		require.NoError(t, err)
		require.NotNil(t, psd)
		assert.Len(t, psd.ParamOIDs, paramCount)
		assert.Len(t, psd.Fields, 1)

		result := pgConn.ExecPrepared(context.Background(), "ps1", args, nil, nil).Read()
		require.EqualError(t, result.Err, "extended protocol limited to 65535 parameters")
	}

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
	assert.Equal(t, pgconn.CommandTag{}, commandTag)
	assert.True(t, pgconn.Timeout(err))
	assert.True(t, pgConn.IsClosed())
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

func TestConnExecPreparedPrecanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Prepare(context.Background(), "ps1", "select current_database(), pg_sleep(1)", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := pgConn.ExecPrepared(ctx, "ps1", nil, nil, nil).Read()
	require.Error(t, result.Err)
	assert.True(t, errors.Is(result.Err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(result.Err))

	ensureConnValid(t, pgConn)
}

func TestConnExecPreparedEmptySQL(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	pgConn, err := pgconn.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Prepare(ctx, "ps1", "", nil)
	require.NoError(t, err)

	result := pgConn.ExecPrepared(ctx, "ps1", nil, nil, nil).Read()
	assert.Equal(t, pgconn.CommandTag{}, result.CommandTag)
	assert.Len(t, result.Rows, 0)
	assert.NoError(t, result.Err)

	ensureConnValid(t, pgConn)
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
	assert.Equal(t, "SELECT 1", results[0].CommandTag.String())

	require.Len(t, results[1].Rows, 1)
	require.Equal(t, "ExecPrepared 1", string(results[1].Rows[0][0]))
	assert.Equal(t, "SELECT 1", results[1].CommandTag.String())

	require.Len(t, results[2].Rows, 1)
	require.Equal(t, "ExecParams 2", string(results[2].Rows[0][0]))
	assert.Equal(t, "SELECT 1", results[2].CommandTag.String())
}

func TestConnExecBatchDeferredError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")
	}

	setupSQL := `create temporary table t (
		id text primary key,
		n int not null,
		unique (n) deferrable initially deferred
	);

	insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);`

	_, err = pgConn.Exec(context.Background(), setupSQL).ReadAll()
	require.NoError(t, err)

	batch := &pgconn.Batch{}

	batch.ExecParams(`update t set n=n+1 where id='b' returning *`, nil, nil, nil, nil)
	_, err = pgConn.ExecBatch(context.Background(), batch).ReadAll()
	require.NotNil(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23505", pgErr.Code)

	ensureConnValid(t, pgConn)
}

func TestConnExecBatchPrecanceled(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pgConn.ExecBatch(ctx, batch).ReadAll()
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(err))

	ensureConnValid(t, pgConn)
}

// Without concurrent reading and writing large batches can deadlock.
//
// See https://github.com/jackc/pgx/issues/374.
func TestConnExecBatchHuge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	batch := &pgconn.Batch{}

	queryCount := 100000
	args := make([]string, queryCount)

	for i := range args {
		args[i] = strconv.Itoa(i)
		batch.ExecParams("select $1::text", [][]byte{[]byte(args[i])}, nil, nil, nil)
	}

	results, err := pgConn.ExecBatch(context.Background(), batch).ReadAll()
	require.NoError(t, err)
	require.Len(t, results, queryCount)

	for i := range args {
		require.Len(t, results[i].Rows, 1)
		require.Equal(t, args[i], string(results[i].Rows[0][0]))
		assert.Equal(t, "SELECT 1", results[i].CommandTag.String())
	}
}

func TestConnExecBatchImplicitTransaction(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Skipping due to known server issue: (https://github.com/cockroachdb/cockroach/issues/44803)")
	}

	_, err = pgConn.Exec(context.Background(), "create temporary table t(id int)").ReadAll()
	require.NoError(t, err)

	batch := &pgconn.Batch{}

	batch.ExecParams("insert into t(id) values(1)", nil, nil, nil, nil)
	batch.ExecParams("insert into t(id) values(2)", nil, nil, nil, nil)
	batch.ExecParams("insert into t(id) values(3)", nil, nil, nil, nil)
	batch.ExecParams("select 1/0", nil, nil, nil, nil)
	_, err = pgConn.ExecBatch(context.Background(), batch).ReadAll()
	require.Error(t, err)

	result := pgConn.ExecParams(context.Background(), "select count(*) from t", nil, nil, nil, nil).Read()
	require.Equal(t, "0", string(result.Rows[0][0]))
}

func TestConnLocking(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	mrr := pgConn.Exec(context.Background(), "select 'Hello, world'")
	_, err = pgConn.Exec(context.Background(), "select 'Hello, world'").ReadAll()
	assert.Error(t, err)
	assert.Equal(t, "conn busy", err.Error())
	assert.True(t, pgconn.SafeToRetry(err))

	results, err := mrr.ReadAll()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Nil(t, results[0].Err)
	assert.Equal(t, "SELECT 1", results[0].CommandTag.String())
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "Hello, world", string(results[0].Rows[0][0]))

	ensureConnValid(t, pgConn)
}

func TestConnOnNotice(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	var msg string
	config.OnNotice = func(c *pgconn.PgConn, notice *pgconn.Notice) {
		msg = notice.Message
	}
	config.RuntimeParams["client_min_messages"] = "notice" // Ensure we only get the message we expect.

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support PL/PGSQL (https://github.com/cockroachdb/cockroach/issues/17511)")
	}

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

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)")
	}

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

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)")
	}

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

func TestConnWaitForNotificationPrecanceled(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = pgConn.WaitForNotification(ctx)
	require.ErrorIs(t, err, context.Canceled)

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
	assert.True(t, pgconn.Timeout(err))
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	ensureConnValid(t, pgConn)
}

func TestConnCopyToSmall(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does support COPY TO")
	}

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

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does support COPY TO")
	}

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

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)")
	}

	outputWriter := &bytes.Buffer{}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := pgConn.CopyTo(ctx, outputWriter, "copy (select *, pg_sleep(0.01) from generate_series(1,1000)) to stdout")
	assert.Error(t, err)
	assert.Equal(t, pgconn.CommandTag{}, res)

	assert.True(t, pgConn.IsClosed())
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

func TestConnCopyToPrecanceled(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	outputWriter := &bytes.Buffer{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := pgConn.CopyTo(ctx, outputWriter, "copy (select * from generate_series(1,1000)) to stdout")
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(err))
	assert.Equal(t, pgconn.CommandTag{}, res)

	ensureConnValid(t, pgConn)
}

func TestConnCopyFrom(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)")
	}

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

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)")
	}

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
	assert.Error(t, err)

	assert.True(t, pgConn.IsClosed())
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

func TestConnCopyFromPrecanceled(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ct, err := pgConn.CopyFrom(ctx, r, "COPY foo FROM STDIN WITH (FORMAT csv)")
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.True(t, pgconn.SafeToRetry(err))
	assert.Equal(t, pgconn.CommandTag{}, ct)

	ensureConnValid(t, pgConn)
}

func TestConnCopyFromGzipReader(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not fully support COPY FROM (https://www.cockroachlabs.com/docs/v20.2/copy-from.html)")
	}

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

	// Send data even though the COPY FROM command will be rejected with a syntax error. This ensures that this does not
	// break the connection. See https://github.com/jackc/pgconn/pull/127 for context.
	inputRows := [][][]byte{}
	for i := 0; i < 1000; i++ {
		a := strconv.Itoa(i)
		b := "foo " + a + " bar"
		inputRows = append(inputRows, [][]byte{[]byte(a), []byte(b)})
		_, err = srcBuf.Write([]byte(fmt.Sprintf("%s,\"%s\"\n", a, b)))
		require.NoError(t, err)
	}

	res, err := pgConn.CopyFrom(context.Background(), srcBuf, "cropy foo FROM STDIN WITH (FORMAT csv)")
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

	res, err := pgConn.CopyFrom(context.Background(), srcBuf, "copy foo to stdout")
	require.Error(t, err)
	assert.IsType(t, &pgconn.PgError{}, err)
	assert.Equal(t, int64(0), res.RowsAffected())

	ensureConnValid(t, pgConn)
}

// https://github.com/jackc/pgconn/issues/21
func TestConnCopyFromNoticeResponseReceivedMidStream(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pgConn, err := pgconn.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support triggers (https://github.com/cockroachdb/cockroach/issues/28296)")
	}

	_, err = pgConn.Exec(ctx, `create temporary table sentences(
		t text,
		ts tsvector
	)`).ReadAll()
	require.NoError(t, err)

	_, err = pgConn.Exec(ctx, `create function pg_temp.sentences_trigger() returns trigger as $$
	begin
	  new.ts := to_tsvector(new.t);
		return new;
	end
	$$ language plpgsql;`).ReadAll()
	require.NoError(t, err)

	_, err = pgConn.Exec(ctx, `create trigger sentences_update before insert on sentences for each row execute procedure pg_temp.sentences_trigger();`).ReadAll()
	require.NoError(t, err)

	longString := make([]byte, 10001)
	for i := range longString {
		longString[i] = 'x'
	}

	buf := &bytes.Buffer{}
	for i := 0; i < 1000; i++ {
		buf.Write([]byte(fmt.Sprintf("%s\n", string(longString))))
	}

	_, err = pgConn.CopyFrom(ctx, buf, "COPY sentences(t) FROM STDIN WITH (FORMAT csv)")
	require.NoError(t, err)
}

type delayedReader struct {
	r io.Reader
}

func (d delayedReader) Read(p []byte) (int, error) {
	// W/o sleep test passes, with sleep it fails.
	time.Sleep(time.Millisecond)
	return d.r.Read(p)
}

// https://github.com/jackc/pgconn/issues/128
func TestConnCopyFromDataWriteAfterErrorAndReturn(t *testing.T) {
	connString := os.Getenv("PGX_TEST_DATABASE")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_DATABASE")
	}

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	pgConn, err := pgconn.ConnectConfig(context.Background(), config)
	require.NoError(t, err)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does support COPY FROM")
	}

	setupSQL := `create temporary table t (
		id text primary key,
		n int not null
	);`

	_, err = pgConn.Exec(context.Background(), setupSQL).ReadAll()
	assert.NoError(t, err)

	r1 := delayedReader{r: strings.NewReader(`id	0\n`)}
	// Generate an error with a bogus COPY command
	_, err = pgConn.CopyFrom(context.Background(), r1, "COPY nosuchtable FROM STDIN ")
	assert.Error(t, err)

	r2 := delayedReader{r: strings.NewReader(`id	0\n`)}
	_, err = pgConn.CopyFrom(context.Background(), r2, "COPY t FROM STDIN")
	assert.NoError(t, err)
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

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support query cancellation (https://github.com/cockroachdb/cockroach/issues/41335)")
	}

	multiResult := pgConn.Exec(context.Background(), "select 'Hello, world', pg_sleep(2)")

	go func() {
		// The query is actually sent when multiResult.NextResult() is called. So wait to ensure it is sent.
		// Once Flush is available this could use that instead.
		time.Sleep(500 * time.Millisecond)

		err := pgConn.CancelRequest(context.Background())
		require.NoError(t, err)
	}()

	for multiResult.NextResult() {
	}
	err = multiResult.Close()

	require.IsType(t, &pgconn.PgError{}, err)
	require.Equal(t, "57014", err.(*pgconn.PgError).Code)

	ensureConnValid(t, pgConn)
}

// https://github.com/jackc/pgx/issues/659
func TestConnContextCanceledCancelsRunningQueryOnServer(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pid := pgConn.PID()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	multiResult := pgConn.Exec(ctx, "select 'Hello, world', pg_sleep(30)")

	for multiResult.NextResult() {
	}
	err = multiResult.Close()
	assert.True(t, pgconn.Timeout(err))
	assert.True(t, pgConn.IsClosed())
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}

	otherConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, otherConn)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	for {
		result := otherConn.ExecParams(ctx,
			`select 1 from pg_stat_activity where pid=$1`,
			[][]byte{[]byte(strconv.FormatInt(int64(pid), 10))},
			nil,
			nil,
			nil,
		).Read()
		require.NoError(t, result.Err)

		if len(result.Rows) == 0 {
			break
		}
	}
}

func TestHijackAndConstruct(t *testing.T) {
	t.Parallel()

	origConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	hc, err := origConn.Hijack()
	require.NoError(t, err)

	_, err = origConn.Exec(context.Background(), "select 'Hello, world'").ReadAll()
	require.Error(t, err)

	newConn, err := pgconn.Construct(hc)
	require.NoError(t, err)

	defer closeConn(t, newConn)

	results, err := newConn.Exec(context.Background(), "select 'Hello, world'").ReadAll()
	assert.NoError(t, err)

	assert.Len(t, results, 1)
	assert.Nil(t, results[0].Err)
	assert.Equal(t, "SELECT 1", results[0].CommandTag.String())
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "Hello, world", string(results[0].Rows[0][0]))

	ensureConnValid(t, newConn)
}

func TestConnCloseWhileCancellableQueryInProgress(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	ctx, _ := context.WithCancel(context.Background())
	pgConn.Exec(ctx, "select n from generate_series(1,10) n")

	closeCtx, _ := context.WithCancel(context.Background())
	pgConn.Close(closeCtx)
	select {
	case <-pgConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

// https://github.com/jackc/pgx/issues/800
func TestFatalErrorReceivedAfterCommandComplete(t *testing.T) {
	t.Parallel()

	steps := pgmock.AcceptUnauthenticatedConnRequestSteps()
	steps = append(steps, pgmock.ExpectAnyMessage(&pgproto3.Parse{}))
	steps = append(steps, pgmock.ExpectAnyMessage(&pgproto3.Bind{}))
	steps = append(steps, pgmock.ExpectAnyMessage(&pgproto3.Describe{}))
	steps = append(steps, pgmock.ExpectAnyMessage(&pgproto3.Execute{}))
	steps = append(steps, pgmock.ExpectAnyMessage(&pgproto3.Sync{}))
	steps = append(steps, pgmock.SendMessage(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		{Name: []byte("mock")},
	}}))
	steps = append(steps, pgmock.SendMessage(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")}))
	steps = append(steps, pgmock.SendMessage(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "57P01"}))

	script := &pgmock.Script{Steps: steps}

	ln, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err)
	defer ln.Close()

	serverErrChan := make(chan error, 1)
	go func() {
		defer close(serverErrChan)

		conn, err := ln.Accept()
		if err != nil {
			serverErrChan <- err
			return
		}
		defer conn.Close()

		err = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			serverErrChan <- err
			return
		}

		err = script.Run(pgproto3.NewBackend(conn, conn))
		if err != nil {
			serverErrChan <- err
			return
		}
	}()

	parts := strings.Split(ln.Addr().String(), ":")
	host := parts[0]
	port := parts[1]
	connStr := fmt.Sprintf("sslmode=disable host=%s port=%s", host, port)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgconn.Connect(ctx, connStr)
	require.NoError(t, err)

	rr := conn.ExecParams(ctx, "mocked...", nil, nil, nil, nil)

	for rr.NextRow() {
	}

	_, err = rr.Close()
	require.Error(t, err)
}

// https://github.com/jackc/pgconn/issues/27
func TestConnLargeResponseWhileWritingDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	_, err = pgConn.Exec(context.Background(), "set client_min_messages = debug5").ReadAll()
	require.NoError(t, err)

	// The actual contents of this test aren't important. What's important is a large amount of data to be written and
	// because of client_min_messages = debug5 the server will return a large amount of data.

	paramCount := math.MaxUint16
	params := make([]string, 0, paramCount)
	args := make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, fmt.Sprintf("($%d::text)", i+1))
		args = append(args, []byte(strconv.Itoa(i)))
	}
	sql := "values" + strings.Join(params, ", ")

	result := pgConn.ExecParams(context.Background(), sql, args, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	require.Len(t, result.Rows, paramCount)

	ensureConnValid(t, pgConn)
}

func TestConnCheckConn(t *testing.T) {
	t.Parallel()

	// Intentionally using TCP connection for more predictable close behavior. (Not sure if Unix domain sockets would behave subtlely different.)

	connString := os.Getenv("PGX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TCP_CONN_STRING")
	}

	c1, err := pgconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	defer c1.Close(context.Background())

	if c1.ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support pg_terminate_backend() (https://github.com/cockroachdb/cockroach/issues/35897)")
	}

	err = c1.CheckConn()
	require.NoError(t, err)

	c2, err := pgconn.Connect(context.Background(), connString)
	require.NoError(t, err)
	defer c2.Close(context.Background())

	_, err = c2.Exec(context.Background(), fmt.Sprintf("select pg_terminate_backend(%d)", c1.PID())).ReadAll()
	require.NoError(t, err)

	// Give a little time for the signal to actually kill the backend.
	time.Sleep(500 * time.Millisecond)

	err = c1.CheckConn()
	require.Error(t, err)
}

func TestPipelinePrepare(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	result := pgConn.ExecParams(context.Background(), `create temporary table t (id text primary key)`, nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendPrepare("selectInt", "select $1::bigint as a", nil)
	pipeline.SendPrepare("selectText", "select $1::text as b", nil)
	pipeline.SendPrepare("selectNoParams", "select 42 as c", nil)
	pipeline.SendPrepare("insertNoResults", "insert into t (id) values ($1)", nil)
	pipeline.SendPrepare("insertNoParamsOrResults", "insert into t (id) values ('foo')", nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	sd, ok := results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 1)
	require.Equal(t, string(sd.Fields[0].Name), "a")
	require.Equal(t, []uint32{pgtype.Int8OID}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	sd, ok = results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 1)
	require.Equal(t, string(sd.Fields[0].Name), "b")
	require.Equal(t, []uint32{pgtype.TextOID}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	sd, ok = results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 1)
	require.Equal(t, string(sd.Fields[0].Name), "c")
	require.Equal(t, []uint32{}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	sd, ok = results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 0)
	require.Equal(t, []uint32{pgtype.TextOID}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	sd, ok = results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 0)
	require.Len(t, sd.ParamOIDs, 0)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	require.Nil(t, results)

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelinePrepareError(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendPrepare("selectInt", "select $1::bigint as a", nil)
	pipeline.SendPrepare("selectError", "bad", nil)
	pipeline.SendPrepare("selectText", "select $1::text as b", nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	sd, ok := results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 1)
	require.Equal(t, string(sd.Fields[0].Name), "a")
	require.Equal(t, []uint32{pgtype.Int8OID}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Nil(t, results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	require.Nil(t, results)

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelinePrepareAndDeallocate(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendPrepare("selectInt", "select $1::bigint as a", nil)
	pipeline.SendDeallocate("selectInt")
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	sd, ok := results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 1)
	require.Equal(t, string(sd.Fields[0].Name), "a")
	require.Equal(t, []uint32{pgtype.Int8OID}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.CloseComplete)
	require.Truef(t, ok, "expected CloseComplete, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	require.Nil(t, results)

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelineQuery(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendQueryParams(`select 1`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 2`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 3`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	pipeline.SendQueryParams(`select 4`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 5`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	rr, ok := results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult := rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "1", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "2", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "3", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "4", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "5", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	require.Nil(t, results)

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelinePrepareQuery(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendPrepare("ps", "select $1::text as msg", nil)
	pipeline.SendQueryPrepared(`ps`, [][]byte{[]byte("hello")}, nil, nil)
	pipeline.SendQueryPrepared(`ps`, [][]byte{[]byte("goodbye")}, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	sd, ok := results.(*pgconn.StatementDescription)
	require.Truef(t, ok, "expected StatementDescription, got: %#v", results)
	require.Len(t, sd.Fields, 1)
	require.Equal(t, string(sd.Fields[0].Name), "msg")
	require.Equal(t, []uint32{pgtype.TextOID}, sd.ParamOIDs)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok := results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult := rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "hello", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "goodbye", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	require.Nil(t, results)

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelineQueryErrorBetweenSyncs(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendQueryParams(`select 1`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 2`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	pipeline.SendQueryParams(`select 3`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 1/(3-n) from generate_series(1,10) n`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 4`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	pipeline.SendQueryParams(`select 5`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 6`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	rr, ok := results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult := rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "1", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "2", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "3", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, readResult.Err, &pgErr)
	require.Equal(t, "22012", pgErr.Code)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "5", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	rr, ok = results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult = rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "6", string(readResult.Rows[0][0]))

	results, err = pipeline.GetResults()
	require.NoError(t, err)
	_, ok = results.(*pgconn.PipelineSync)
	require.Truef(t, ok, "expected PipelineSync, got: %#v", results)

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelineCloseReadsUnreadResults(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendQueryParams(`select 1`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 2`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 3`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	pipeline.SendQueryParams(`select 4`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 5`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	rr, ok := results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult := rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "1", string(readResult.Rows[0][0]))

	err = pipeline.Close()
	require.NoError(t, err)

	ensureConnValid(t, pgConn)
}

func TestPipelineCloseDetectsUnsyncedRequests(t *testing.T) {
	t.Parallel()

	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	pipeline := pgConn.StartPipeline(context.Background())
	pipeline.SendQueryParams(`select 1`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 2`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 3`, nil, nil, nil, nil)
	err = pipeline.Sync()
	require.NoError(t, err)

	pipeline.SendQueryParams(`select 4`, nil, nil, nil, nil)
	pipeline.SendQueryParams(`select 5`, nil, nil, nil, nil)

	results, err := pipeline.GetResults()
	require.NoError(t, err)
	rr, ok := results.(*pgconn.ResultReader)
	require.Truef(t, ok, "expected ResultReader, got: %#v", results)
	readResult := rr.Read()
	require.NoError(t, readResult.Err)
	require.Len(t, readResult.Rows, 1)
	require.Len(t, readResult.Rows[0], 1)
	require.Equal(t, "1", string(readResult.Rows[0][0]))

	err = pipeline.Close()
	require.EqualError(t, err, "pipeline has unsynced requests")
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

func GetSSLPassword(ctx context.Context) string {
	connString := os.Getenv("PGX_SSL_PASSWORD")
	return connString
}

var rsaCertPEM = `-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUQDlN1g1bzxIJ8KWkayNcQY5gzMEwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTIyMDgxNTIxNDgyNloXDTIzMDgx
NTIxNDgyNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA0vOppiT8zE+076acRORzD5JVbRYKMK3XlWLVrHua4+ct
Rm54WyP+3XsYU4JGGGKgb8E+u2UosGJYcSM+b+U1/5XPTcpuumS+pCiD9WP++A39
tsukYwR7m65cgpiI4dlLEZI3EWpAW+Bb3230KiYW4sAmQ0Ih4PrN+oPvzcs86F4d
9Y03CqVUxRKLBLaClZQAg8qz2Pawwj1FKKjDX7u2fRVR0wgOugpCMOBJMcCgz9pp
0HSa4x3KZDHEZY7Pah5XwWrCfAEfRWsSTGcNaoN8gSxGFM1JOEJa8SAuPGjFcYIv
MmVWdw0FXCgYlSDL02fzLE0uyvXBDibzSqOk770JhQIDAQABo1MwUTAdBgNVHQ4E
FgQUiJ8JLENJ+2k1Xl4o6y2Lc/qHTh0wHwYDVR0jBBgwFoAUiJ8JLENJ+2k1Xl4o
6y2Lc/qHTh0wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAwjn2
gnNAhFvh58VqLIjU6ftvn6rhz5B9dg2+XyY8sskLhhkO1nL9339BVZsRt+eI3a7I
81GNIm9qHVM3MUAcQv3SZy+0UPVUT8DNH2LwHT3CHnYTBP8U+8n8TDNGSTMUhIBB
Rx+6KwODpwLdI79VGT3IkbU9bZwuepB9I9nM5t/tt5kS4gHmJFlO0aLJFCTO4Scf
hp/WLPv4XQUH+I3cPfaJRxz2j0Kc8iOzMhFmvl1XOGByjX6X33LnOzY/LVeTSGyS
VgC32BGtnMwuy5XZYgFAeUx9HKy4tG4OH2Ux6uPF/WAhsug6PXSjV7BK6wYT5i27
MlascjupnaptKX/wMA==
-----END CERTIFICATE-----
`

var rsaKeyPEM = testingKey(`-----BEGIN TESTING KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDS86mmJPzMT7Tv
ppxE5HMPklVtFgowrdeVYtWse5rj5y1GbnhbI/7dexhTgkYYYqBvwT67ZSiwYlhx
Iz5v5TX/lc9Nym66ZL6kKIP1Y/74Df22y6RjBHubrlyCmIjh2UsRkjcRakBb4Fvf
bfQqJhbiwCZDQiHg+s36g+/NyzzoXh31jTcKpVTFEosEtoKVlACDyrPY9rDCPUUo
qMNfu7Z9FVHTCA66CkIw4EkxwKDP2mnQdJrjHcpkMcRljs9qHlfBasJ8AR9FaxJM
Zw1qg3yBLEYUzUk4QlrxIC48aMVxgi8yZVZ3DQVcKBiVIMvTZ/MsTS7K9cEOJvNK
o6TvvQmFAgMBAAECggEAKzTK54Ol33bn2TnnwdiElIjlRE2CUswYXrl6iDRc2hbs
WAOiVRB/T/+5UMla7/2rXJhY7+rdNZs/ABU24ZYxxCJ77jPrD/Q4c8j0lhsgCtBa
ycjV543wf0dsHTd+ubtWu8eVzdRUUD0YtB+CJevdPh4a+CWgaMMV0xyYzi61T+Yv
Z7Uc3awIAiT4Kw9JRmJiTnyMJg5vZqW3BBAX4ZIvS/54ipwEU+9sWLcuH7WmCR0B
QCTqS6hfJDLm//dGC89Iyno57zfYuiT3PYCWH5crr/DH3LqnwlNaOGSBkhkXuIL+
QvOaUMe2i0pjqxDrkBx05V554vyy9jEvK7i330HL4QKBgQDUJmouEr0+o7EMBApC
CPPu58K04qY5t9aGciG/pOurN42PF99yNZ1CnynH6DbcnzSl8rjc6Y65tzTlWods
bjwVfcmcokG7sPcivJvVjrjKpSQhL8xdZwSAjcqjN4yoJ/+ghm9w+SRmZr6oCQZ3
1jREfJKT+PGiWTEjYcExPWUD2QKBgQD+jdgq4c3tFavU8Hjnlf75xbStr5qu+fp2
SGLRRbX+msQwVbl2ZM9AJLoX9MTCl7D9zaI3ONhheMmfJ77lDTa3VMFtr3NevGA6
MxbiCEfRtQpNkJnsqCixLckx3bskj5+IF9BWzw7y7nOzdhoWVFv/+TltTm3RB51G
McdlmmVjjQKBgQDSFAw2/YV6vtu2O1XxGC591/Bd8MaMBziev+wde3GHhaZfGVPC
I8dLTpMwCwowpFKdNeLLl1gnHX161I+f1vUWjw4TVjVjaBUBx+VEr2Tb/nXtiwiD
QV0a883CnGJjreAblKRMKdpasMmBWhaWmn39h6Iad3zHuCzJjaaiXNpn2QKBgQCf
k1Q8LanmQnuh1c41f7aD5gjKCRezMUpt9BrejhD1NxheJJ9LNQ8nat6uPedLBcUS
lmJms+AR2qKqf0QQWyQ98YgAtshgTz8TvQtPT1mWgSOgVFHqJdC8obNK63FyDgc4
TZVxlgQNDqbBjfv0m5XA9f+mIlB9hYR2iKYzb4K30QKBgQC+LEJYZh00zsXttGHr
5wU1RzbgDIEsNuu+nZ4MxsaCik8ILNRHNXdeQbnADKuo6ATfhdmDIQMVZLG8Mivi
UwnwLd1GhizvqvLHa3ULnFphRyMGFxaLGV48axTT2ADoMX67ILrIY/yjycLqRZ3T
z3w+CgS20UrbLIR1YXfqUXge1g==
-----END TESTING KEY-----
`)

func testingKey(s string) string { return strings.ReplaceAll(s, "TESTING KEY", "PRIVATE KEY") }

func TestSNISupport(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sni_param string
		sni_set   bool
	}{
		{
			name:      "SNI is passed by default",
			sni_param: "",
			sni_set:   true,
		},
		{
			name:      "SNI is passed when asked for",
			sni_param: "sslsni=1",
			sni_set:   true,
		},
		{
			name:      "SNI is not passed when disabled",
			sni_param: "sslsni=0",
			sni_set:   false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ln, err := net.Listen("tcp", "127.0.0.1:")
			require.NoError(t, err)
			defer ln.Close()

			serverErrChan := make(chan error, 1)
			serverSNINameChan := make(chan string, 1)
			defer close(serverErrChan)
			defer close(serverSNINameChan)

			go func() {
				var sniHost string

				conn, err := ln.Accept()
				if err != nil {
					serverErrChan <- err
					return
				}
				defer conn.Close()

				err = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if err != nil {
					serverErrChan <- err
					return
				}

				backend := pgproto3.NewBackend(conn, conn)
				startupMessage, err := backend.ReceiveStartupMessage()
				if err != nil {
					serverErrChan <- err
					return
				}

				switch startupMessage.(type) {
				case *pgproto3.SSLRequest:
					_, err = conn.Write([]byte("S"))
					if err != nil {
						serverErrChan <- err
						return
					}
				default:
					serverErrChan <- fmt.Errorf("unexpected startup message: %#v", startupMessage)
					return
				}

				cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
				if err != nil {
					serverErrChan <- err
					return
				}

				srv := tls.Server(conn, &tls.Config{
					Certificates: []tls.Certificate{cert},
					GetConfigForClient: func(argHello *tls.ClientHelloInfo) (*tls.Config, error) {
						sniHost = argHello.ServerName
						return nil, nil
					},
				})
				defer srv.Close()

				if err := srv.Handshake(); err != nil {
					serverErrChan <- fmt.Errorf("handshake: %v", err)
					return
				}

				srv.Write((&pgproto3.AuthenticationOk{}).Encode(nil))
				srv.Write((&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0}).Encode(nil))
				srv.Write((&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil))

				serverSNINameChan <- sniHost
			}()

			port := strings.Split(ln.Addr().String(), ":")[1]
			connStr := fmt.Sprintf("sslmode=require host=localhost port=%s %s", port, tt.sni_param)
			_, err = pgconn.Connect(context.Background(), connStr)

			select {
			case sniHost := <-serverSNINameChan:
				if tt.sni_set {
					require.Equal(t, sniHost, "localhost")
				} else {
					require.Equal(t, sniHost, "")
				}
			case err = <-serverErrChan:
				t.Fatalf("server failed with error: %+v", err)
			case <-time.After(time.Millisecond * 100):
				t.Fatal("exceeded connection timeout without erroring out")
			}
		})
	}
}
