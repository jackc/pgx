package pgconn

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// connectAuthTest runs the real startup loop against a scripted server. The
// server must finish by reading either Terminate or EOF from the client.
func connectAuthTest(t *testing.T, config *Config, serve func(*pgproto3.Backend) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		backend := pgproto3.NewBackend(server, server)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			done <- err
			return
		}
		done <- serve(backend)
	}()
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	conn, err := ConnectConfig(ctx, config)
	if conn != nil {
		require.NoError(t, conn.Close(ctx))
	}
	require.NoError(t, <-done)
	return err
}

func TestConnectRequireAuthCompletion(t *testing.T) {
	for _, policy := range []string{"scram-sha-256", "password", "!none", "", "none", "scram-sha-256,none", "!password"} {
		for _, authOK := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/authOK=%v", policy, authOK), func(t *testing.T) {
				config, err := ParseConfig("host=127.0.0.1 user=test password=test sslmode=disable")
				require.NoError(t, err)
				config.RequireAuth = policy
				mustReject := policy == "scram-sha-256" || policy == "password" || policy == "!none"
				validated := false
				config.ValidateConnect = func(context.Context, *PgConn) error { validated = true; return nil }
				err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
					if authOK {
						b.Send(&pgproto3.AuthenticationOk{})
					}
					b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := b.Flush(); err != nil {
						return err
					}
					msg, err := b.Receive()
					if mustReject {
						if err == nil {
							return fmt.Errorf("expected connection closure, got %T", msg)
						}
					} else if _, ok := msg.(*pgproto3.Terminate); !ok {
						return fmt.Errorf("expected Terminate, got %T: %v", msg, err)
					}
					return nil
				})
				if mustReject {
					require.ErrorContains(t, err, "server did not complete authentication")
					require.False(t, validated)
				} else {
					require.NoError(t, err)
					require.True(t, validated)
				}
			})
		}
	}
}

func TestConnectRequireAuthPasswordCompleted(t *testing.T) {
	config, err := ParseConfig("host=127.0.0.1 user=test password=test sslmode=disable require_auth=password")
	require.NoError(t, err)
	err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
		b.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := b.Flush(); err != nil {
			return err
		}
		msg, err := b.Receive()
		if password, ok := msg.(*pgproto3.PasswordMessage); !ok || password.Password != "test" {
			return fmt.Errorf("expected password, got %T: %v", msg, err)
		}
		b.Send(&pgproto3.AuthenticationOk{})
		b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := b.Flush(); err != nil {
			return err
		}
		msg, err = b.Receive()
		if _, ok := msg.(*pgproto3.Terminate); !ok {
			return fmt.Errorf("expected Terminate, got %T: %v", msg, err)
		}
		return nil
	})
	require.NoError(t, err)
}
