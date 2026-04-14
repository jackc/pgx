package pgconn_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildCancelAndDrainConfig(t *testing.T) *pgconn.Config {
	t.Helper()
	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelAndDrainContextWatcherHandler{Conn: conn}
	}
	config.ConnectTimeout = 5 * time.Second
	return config
}

func TestCancelAndDrainContextWatcherHandler(t *testing.T) {
	t.Parallel()

	t.Run("connection reused after cancel", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err = pgConn.Exec(ctx, "select pg_sleep(10)").ReadAll()
		require.Error(t, err)
		require.False(t, pgConn.IsClosed(), "connection should not be closed after cancel with drain handler")

		ensureConnValid(t, pgConn)
	})

	t.Run("no stale cancel bleed", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		for i := range 50 {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				defer cancel()
				pgConn.Exec(ctx, "select pg_sleep(0.020)").ReadAll()
			}()

			if pgConn.IsClosed() {
				var err error
				pgConn, err = pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
				require.NoError(t, err, "iteration %d: failed to reconnect after closed connection", i)
			}

			ensureConnValid(t, pgConn)
		}
	})

	t.Run("stress", func(t *testing.T) {
		t.Parallel()

		for i := range 10 {
			t.Run(fmt.Sprintf("goroutine_%d", i), func(t *testing.T) {
				t.Parallel()

				pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
				require.NoError(t, err)
				defer closeConn(t, pgConn)

				for j := range 20 {
					func() {
						ctx, cancel := context.WithTimeout(context.Background(), 4*time.Millisecond)
						defer cancel()
						pgConn.Exec(ctx, "select pg_sleep(0.010)").ReadAll()
					}()

					if pgConn.IsClosed() {
						var err error
						pgConn, err = pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
						require.NoError(t, err, "goroutine %d iteration %d: failed to reconnect", i, j)
					}

					ensureConnValid(t, pgConn)
				}
			})
		}
	})

	t.Run("ExecParams", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		rr := pgConn.ExecParams(ctx, "select pg_sleep(10)", nil, nil, nil, nil)
		rr.Read()
		_, err = rr.Close()
		assert.Error(t, err)

		if !pgConn.IsClosed() {
			ensureConnValid(t, pgConn)
		}
	})

	t.Run("CopyTo", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err = pgConn.CopyTo(ctx, io.Discard, "COPY (SELECT pg_sleep(10)) TO STDOUT")
		assert.Error(t, err)

		if !pgConn.IsClosed() {
			ensureConnValid(t, pgConn)
		}
	})

	t.Run("CopyFrom", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		_, err = pgConn.Exec(context.Background(), "CREATE TEMP TABLE drain_test_copyfrom (id int)").ReadAll()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		pr, pw := io.Pipe()
		defer pr.Close()
		defer pw.Close()

		_, err = pgConn.CopyFrom(ctx, pr, "COPY drain_test_copyfrom FROM STDIN")
		assert.Error(t, err)

		if !pgConn.IsClosed() {
			ensureConnValid(t, pgConn)
		}
	})

	t.Run("Pipeline", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		pipeline := pgConn.StartPipeline(ctx)

		pipeline.SendQueryParams("select pg_sleep(10)", nil, nil, nil, nil)
		err = pipeline.Sync()
		require.NoError(t, err)

		pipeline.Close()

		require.False(t, pgConn.IsClosed(), "connection should not be closed after cancelled pipeline with drain handler")
		ensureConnValid(t, pgConn)
	})

	t.Run("Prepare", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		for i := range 20 {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				defer cancel()
				pgConn.Prepare(ctx, "", "select pg_sleep(0.010)", nil)
			}()

			if pgConn.IsClosed() {
				var err error
				pgConn, err = pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
				require.NoError(t, err, "iteration %d: failed to reconnect after closed connection", i)
			}

			ensureConnValid(t, pgConn)
		}
	})

	t.Run("Deallocate", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		for i := range 20 {
			_, err := pgConn.Prepare(context.Background(), "drain_dealloc_test", "select 1", nil)
			require.NoError(t, err, "iteration %d: prepare failed", i)

			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				defer cancel()
				pgConn.Deallocate(ctx, "drain_dealloc_test")
			}()

			if pgConn.IsClosed() {
				var err error
				pgConn, err = pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
				require.NoError(t, err, "iteration %d: failed to reconnect after closed connection", i)
			}

			ensureConnValid(t, pgConn)
		}
	})

	t.Run("WaitForNotification", func(t *testing.T) {
		t.Parallel()

		pgConn, err := pgconn.ConnectConfig(context.Background(), buildCancelAndDrainConfig(t))
		require.NoError(t, err)
		defer closeConn(t, pgConn)

		if pgConn.ParameterStatus("crdb_version") != "" {
			t.Skip("Server does not support LISTEN / NOTIFY (https://github.com/cockroachdb/cockroach/issues/41522)")
		}

		_, err = pgConn.Exec(context.Background(), "LISTEN drain_test_channel").ReadAll()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err = pgConn.WaitForNotification(ctx)
		require.Error(t, err)

		require.False(t, pgConn.IsClosed(), "connection should not be closed after cancelled WaitForNotification with drain handler")
		ensureConnValid(t, pgConn)
	})
}
