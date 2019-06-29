package pgxpool_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestConnExec(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer c.Release()

	testExec(t, c)
}

func TestConnQuery(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer c.Release()

	testQuery(t, c)
}

func TestConnQueryRow(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer c.Release()

	testQueryRow(t, c)
}

func TestConnSendBatch(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer c.Release()

	testSendBatch(t, c)
}

func TestConnCopyFrom(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer c.Release()

	testCopyFrom(t, c)
}
