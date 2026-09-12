package pgxpool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolSendBatchBeginTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1")

	br := pool.SendBatch(ctx, batch)
	tx := br.Tx()
	require.NotNil(t, tx)
	defer tx.Rollback(ctx)
	require.NoError(t, br.Close())

	// Close leaves the connection acquired for the transaction.
	require.EqualValues(t, 1, pool.Stat().AcquiredConns())

	var n int32
	require.NoError(t, tx.QueryRow(ctx, "select 2").Scan(&n))
	require.EqualValues(t, 2, n)

	require.NoError(t, tx.Commit(ctx))
	waitForReleaseToComplete()
	require.EqualValues(t, 0, pool.Stat().AcquiredConns())
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestPoolSendBatchBeginTxRollbackAfterQueryError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1/0")

	br := pool.SendBatch(ctx, batch)
	tx := br.Tx()
	require.Error(t, br.Close())
	require.EqualValues(t, 1, pool.Stat().AcquiredConns())

	require.NoError(t, tx.Rollback(ctx))
	waitForReleaseToComplete()
	require.EqualValues(t, 0, pool.Stat().AcquiredConns())
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestPoolSendBatchBeginTxReleasesConnWhenBeginFails(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{BeginQuery: "begin garbage"})
	batch.Queue("select 1")

	br := pool.SendBatch(ctx, batch)
	tx := br.Tx()
	require.Error(t, br.Close())

	// The transaction never began, so it is already closed, but rolling it back still releases the connection.
	require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)
	waitForReleaseToComplete()
	require.EqualValues(t, 0, pool.Stat().AcquiredConns())
	require.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestPoolSendBatchTxIsNilWithoutBeginTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	batch := &pgx.Batch{}
	batch.Queue("select 1")

	br := pool.SendBatch(ctx, batch)
	require.Nil(t, br.Tx())
	require.NoError(t, br.Close())
	waitForReleaseToComplete()
	require.EqualValues(t, 0, pool.Stat().AcquiredConns())

	pool.Close()
	require.Nil(t, pool.SendBatch(ctx, batch).Tx())
}

func TestConnSendBatchBeginTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1")

	br := c.SendBatch(ctx, batch)
	tx := br.Tx()
	require.NotNil(t, tx)
	require.NoError(t, br.Close())
	require.EqualValues(t, 'T', c.Conn().PgConn().TxStatus())
	require.NoError(t, tx.Commit(ctx))

	// The caller still owns the connection: committing the transaction does not release it.
	require.EqualValues(t, 1, pool.Stat().AcquiredConns())
}

func TestPoolSendBatchBeginTxCloseAgainAfterCommit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.MaxConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1")

	br := pool.SendBatch(ctx, batch)
	require.NoError(t, br.Close())
	require.NoError(t, br.Tx().Commit(ctx))

	// The only connection is back in the pool and in use by another goroutine. Closing the results again, as a
	// deferred Close does, must not touch it. The race detector catches a violation.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 50 {
			_, err := pool.Exec(ctx, "select 1")
			assert.NoError(t, err)
		}
	}()
	for range 50 {
		require.NoError(t, br.Close())
	}
	<-done
}

func TestPoolSendBatchBeginTxAcquireError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.MaxConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	// Hold the only connection so that SendBatch cannot acquire one.
	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1")

	acquireCtx, cancelAcquire := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelAcquire()
	br := pool.SendBatch(acquireCtx, batch)

	// The transaction handle exists but is closed, so the usual deferred Rollback is safe.
	tx := br.Tx()
	require.NotNil(t, tx)
	require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)
	require.ErrorIs(t, br.Close(), context.DeadlineExceeded)
}
