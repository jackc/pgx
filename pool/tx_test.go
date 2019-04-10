package pool_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/pool"
	"github.com/stretchr/testify/require"
)

func TestTxExec(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testExec(t, tx)
}

func TestTxQuery(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testQuery(t, tx)
}

func TestTxQueryEx(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testQueryEx(t, tx)
}

func TestTxQueryRow(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testQueryRow(t, tx)
}

func TestTxQueryRowEx(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	tx, err := pool.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	testQueryRowEx(t, tx)
}
