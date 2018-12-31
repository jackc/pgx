package pgconn_test

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgconn"

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

			err = conn.Close()
			require.Nil(t, err)
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

	err = conn.Close()
	require.Nil(t, err)
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
		conn.Close()
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
		conn.Close()
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
	conn.Close()
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

	// TODO - refactor these selects once there are higher level query functions

	conn.SendExec("show application_name")
	conn.SendExec("show search_path")
	err = conn.Flush()
	require.Nil(t, err)

	result := conn.GetResult()
	require.NotNil(t, result)

	rowFound := result.NextRow()
	assert.True(t, rowFound)
	if rowFound {
		assert.Equal(t, "pgxtest", string(result.Value(0)))
	}

	_, err = result.Close()
	assert.Nil(t, err)

	result = conn.GetResult()
	require.NotNil(t, result)

	rowFound = result.NextRow()
	assert.True(t, rowFound)
	if rowFound {
		assert.Equal(t, "myschema", string(result.Value(0)))
	}

	_, err = result.Close()
	assert.Nil(t, err)
}

func TestSimple(t *testing.T) {
	pgConn, err := pgconn.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.Nil(t, err)

	pgConn.SendExec("select current_database()")
	err = pgConn.Flush()
	require.Nil(t, err)

	result := pgConn.GetResult()
	require.NotNil(t, result)

	rowFound := result.NextRow()
	assert.True(t, rowFound)
	if rowFound {
		assert.Equal(t, "pgx_test", string(result.Value(0)))
	}

	_, err = result.Close()
	assert.Nil(t, err)

	err = pgConn.Close()
	assert.Nil(t, err)
}
