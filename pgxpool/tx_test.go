package pgxpool_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestTxExec(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testExec(t, tx)
}

func TestTxQuery(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testQuery(t, tx)
}

func TestTxQueryRow(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testQueryRow(t, tx)
}

func TestTxSendBatch(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testSendBatch(t, tx)
}

func TestTxCopyFrom(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testCopyFrom(t, tx)
}
