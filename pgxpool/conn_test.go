package pgxpool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestConnExec(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	testExec(t, ctx, c)
}

func TestConnQuery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	testQuery(t, ctx, c)
}

func TestConnQueryRow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	testQueryRow(t, ctx, c)
}

func TestConnSendBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	testSendBatch(t, ctx, c)
}

func TestConnCopyFrom(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()

	testCopyFrom(t, ctx, c)
}
