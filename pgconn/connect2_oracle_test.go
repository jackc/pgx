package pgconn

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/pgmock"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestConnect2LibpqOracle runs the same scripted endpoints against both clients.
// Numeric addresses deliberately make no differential DNS claim. The helper
// checks its linked version; see CONNECTION_STRATEGY.md for the pinned build.
func TestConnect2LibpqOracle(t *testing.T) {
	helper := os.Getenv("PGX_TEST_LIBPQ_CONNECT2")
	if helper == "" {
		t.Skip("optional libpq 18.6 oracle is not configured")
	}
	for _, tc := range []struct {
		name, mode, code string
		authOK           bool
		wantCalls        int
		wantOK           bool
	}{
		{"plain-53300", "disable", "53300", false, 1, false},
		{"plain-28P01", "disable", "28P01", false, 1, false},
		{"plain-57P03", "disable", "57P03", false, 2, true},
		{"after-auth-57P03", "disable", "57P03", true, 1, false},
		{"prefer-N", "prefer", "", false, 1, true},
		{"allow-28P01", "allow", "28P01", false, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connect2TestEnv(t)
			run := func(oracle bool) (bool, int) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				var wg sync.WaitGroup
				t.Cleanup(func() { listener.Close(); wg.Wait() })
				var mu sync.Mutex
				count := 0
				wg.Go(func() {
					for {
						conn, err := listener.Accept()
						if err != nil {
							return
						}
						mu.Lock()
						count++
						index := count
						mu.Unlock()
						wg.Go(func() {
							defer conn.Close()
							conn.SetDeadline(time.Now().Add(5 * time.Second))
							b := pgproto3.NewBackend(conn, conn)
							msg, err := b.ReceiveStartupMessage()
							if err != nil {
								return
							}
							if _, ok := msg.(*pgproto3.SSLRequest); ok {
								conn.Write([]byte{'N'})
								if tc.mode == "allow" {
									return
								}
								if _, err = b.ReceiveStartupMessage(); err != nil {
									return
								}
							}
							if index == 1 && tc.code != "" {
								if tc.authOK {
									b.Send(&pgproto3.AuthenticationOk{})
								}
								b.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: tc.code, Message: "fixture"})
								b.Flush()
								return
							}
							b.Send(&pgproto3.AuthenticationOk{})
							b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
							b.Flush()
							pgmock.WaitForClose().Step(b)
						})
					}
				})
				_, port, _ := net.SplitHostPort(listener.Addr().String())
				input := fmt.Sprintf("host=127.0.0.1,127.0.0.1 port=%s user=test dbname=test sslmode=%s connect_timeout=2", port, tc.mode)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ok := false
				if oracle {
					out, err := exec.CommandContext(ctx, helper, input).CombinedOutput()
					require.NoError(t, err, string(out))
					ok = strings.TrimSpace(string(out)) == "connected"
				} else {
					c, err := Connect2(ctx, input, ConnectOptions2{})
					ok = err == nil
					if c != nil {
						c.Close(ctx)
					}
				}
				listener.Close()
				wg.Wait()
				return ok, count
			}
			expectedOK, expectedCount := run(true)
			require.Equal(t, tc.wantOK, expectedOK)
			require.Equal(t, tc.wantCalls, expectedCount)
			actualOK, actualCount := run(false)
			require.Equal(t, expectedOK, actualOK)
			require.Equal(t, expectedCount, actualCount)
		})
	}
}
