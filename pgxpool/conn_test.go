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

func TestConnExecWithOptions(t *testing.T) {
	t.Parallel()
    var sslOptions pgxpool.ParseConfigOptions
	sslOptions.GetSSLPassword = GetSSLPassword
	pool, err := pgxpool.ConnectWithOptions(context.Background(), os.Getenv("PGX_TEST_DATABASE"),sslOptions)
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

func GetSSLPassword(ctx context.Context) string {
	connString := os.Getenv("PGX_SSL_PASSWORD")
    return connString
}
