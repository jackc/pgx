package pool_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pool"
	"github.com/stretchr/testify/require"
)

func TestTxExec(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testExec(t, tx)
}

func TestTxQuery(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testQuery(t, tx)
}

func TestTxQueryRow(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	testQueryRow(t, tx)
}
