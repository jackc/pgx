package pgconn

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() {
	os.Setenv("PGX_TEST_DATABASE", "")
	os.Setenv("PGHOST", "localhost")
	os.Setenv("PGUSER", "postgres")
	os.Setenv("PGPASSWORD", "password")
}

func mustConnect(t *testing.T) *PgConn {
	t.Helper()

	conn, err := Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	return conn
}

func TestCleanup(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		name     string
		testCase func(t *testing.T, conn *PgConn)
	}{
		{
			name: "success",
			testCase: func(t *testing.T, conn *PgConn) {
				ctx := context.Background()

				execCtx, cancel := context.WithTimeout(ctx, time.Millisecond*500)
				defer cancel()

				// expecting error because timeout is less than execution time
				rr := conn.Exec(execCtx, `select pg_sleep(1)`)
				err := rr.Close()
				require.Error(t, err)

				conn.WaitForRecover()
				require.True(t, conn.status == connStatusIdle)

				// checking that socket is clean, and we are reading data from our request, not from the previous one
				rr = conn.Exec(ctx, `select 'goodbye world'`)
				res, err := rr.ReadAll()
				require.NoError(t, err)

				require.True(t, string(res[0].Rows[0][0]) == "goodbye world")
			},
		},
		{
			name: "success tx",
			testCase: func(t *testing.T, conn *PgConn) {
				ctx := context.Background()

				rr := conn.Exec(ctx, `begin;`)
				err := rr.Close()
				require.NoError(t, err)

				execCtx, cancel := context.WithTimeout(ctx, time.Millisecond*500)
				defer cancel()

				// expecting error because timeout is less than execution time
				rr = conn.Exec(execCtx, `select pg_sleep(1)`)
				err = rr.Close()
				require.Error(t, err)

				conn.WaitForRecover()
				require.True(t, conn.status == connStatusIdle)
				require.True(t, conn.txStatus == 'I')

				// checking that socket is clean, and we are reading data from our request, not from the previous one
				rr = conn.Exec(ctx, `select 'goodbye world'`)
				res, err := rr.ReadAll()
				require.NoError(t, err)

				require.True(t, string(res[0].Rows[0][0]) == "goodbye world")
			},
		},
		{
			name: "failed cleanup timepout",
			testCase: func(t *testing.T, conn *PgConn) {
				ctx := context.Background()

				conn.config.RecoverTimeout = time.Millisecond * 200
				conn.config.OnRecover = nil

				execCtx, cancel := context.WithTimeout(ctx, time.Millisecond*300)
				defer cancel()

				// expecting error because timeout is less than execution time
				rr := conn.Exec(execCtx, `select pg_sleep(1)`)
				err := rr.Close()
				require.Error(t, err)

				conn.WaitForRecover()
				// we expect connection to be in closed state as we didnt have enough time for recover
				require.True(t, conn.status == connStatusClosed)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := mustConnect(t)
			tt.testCase(t, conn)
		})
	}
}
