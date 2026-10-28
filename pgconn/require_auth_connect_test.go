package pgconn_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// requireAuthTestServer answers the startup message with startupMessages over a pipe, so the test
// does not depend on a database or listening sockets.
func requireAuthTestServer(t *testing.T, config *pgconn.Config, startupMessages ...pgproto3.BackendMessage) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			backend := pgproto3.NewBackend(server, server)
			if _, err := backend.ReceiveStartupMessage(); err != nil {
				result <- err
				return
			}
			for _, msg := range startupMessages {
				backend.Send(msg)
			}
			result <- backend.Flush()
		}()
		return client, nil
	}
	return result
}

func TestConnectRequireAuthWithoutAuthenticationOk(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		requireAuth string
		wantErr     bool
	}{
		{"", false},
		{"none", false},
		{"!password", false},
		{"scram-sha-256", true},
		{"password,md5", true},
		{"!none", true},
	} {
		t.Run("require_auth="+tc.requireAuth, func(t *testing.T) {
			config, err := pgconn.ParseConfig("host=127.0.0.1 user=test password=secret sslmode=disable")
			require.NoError(t, err)
			config.RequireAuth = tc.requireAuth
			// The server never requests authentication and never sends AuthenticationOk.
			serverDone := requireAuthTestServer(t, config,
				&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{1, 2, 3, 4}},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			)
			config.ValidateConnect = func(context.Context, *pgconn.PgConn) error {
				if tc.wantErr {
					t.Error("require_auth must be checked before ValidateConnect")
				}
				return nil
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.ConnectConfig(ctx, config)
			if tc.wantErr {
				require.ErrorContains(t, err, "server did not complete authentication")
			} else {
				require.NoError(t, err)
				conn.Close(ctx)
			}
			require.NoError(t, <-serverDone)
		})
	}
}
