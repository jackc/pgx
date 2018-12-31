package pgconn_test

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx"
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
	connString := os.Getenv("PGX_TEST_TLS_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_TLS_CONN_STRING")
	}

	conn, err := pgconn.Connect(context.Background(), connString)
	require.Nil(t, err)

	if _, ok := conn.NetConn.(*tls.Conn); !ok {
		t.Error("not a TLS connection")
	}

	closeConn(t, conn)
}

func TestConnectInvalidUser(t *testing.T) {
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
	pgErr, ok := err.(pgx.PgError)
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
	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	config.RuntimeParams = map[string]string{
		"application_name": "pgxtest",
		"search_path":      "myschema",
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	require.Nil(t, err)
	defer closeConn(t, conn)

	result, err := conn.Exec(context.Background(), "show application_name")
	require.Nil(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "pgxtest", string(result.Rows[0][0]))

	result, err = conn.Exec(context.Background(), "show search_path")
	require.Nil(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "myschema", string(result.Rows[0][0]))
}

func TestConnectWithFallback(t *testing.T) {
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
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	result, err := pgConn.Exec(context.Background(), "select current_database()")
	require.Nil(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, pgConn.Config.Database, string(result.Rows[0][0]))
}

func TestConnExecMultipleQueries(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	result, err := pgConn.Exec(context.Background(), "select current_database(); select 1")
	require.Nil(t, err)
	assert.Equal(t, 1, len(result.Rows))
	assert.Equal(t, "1", string(result.Rows[0][0]))
}

func TestConnExecMultipleQueriesError(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	result, err := pgConn.Exec(context.Background(), "select 1; select 1/0; select 1")
	require.NotNil(t, err)
	require.Nil(t, result)
	if pgErr, ok := err.(pgconn.PgError); ok {
		assert.Equal(t, "22012", pgErr.Code)
	} else {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConnExecContextCanceled(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result, err := pgConn.Exec(ctx, "select current_database(), pg_sleep(1)")
	require.Nil(t, result)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestConnRecoverFromTimeout(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)
	defer closeConn(t, pgConn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	result, err := pgConn.Exec(ctx, "select current_database(), pg_sleep(1)")
	cancel()
	require.Nil(t, result)
	assert.Equal(t, context.DeadlineExceeded, err)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if assert.True(t, pgConn.RecoverFromTimeout(ctx)) {
		result, err := pgConn.Exec(ctx, "select 1")
		require.Nil(t, err)
		assert.Len(t, result.Rows, 1)
		assert.Len(t, result.Rows[0], 1)
		assert.Equal(t, "1", string(result.Rows[0][0]))
	}
	cancel()
}
