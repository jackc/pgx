package pool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	pool.Close()
}

func TestParseConfigExtractsPoolArguments(t *testing.T) {
	config, err := pool.ParseConfig("pool_max_conns=42")
	assert.NoError(t, err)
	assert.EqualValues(t, 42, config.MaxConns)
	assert.NotContains(t, config.ConnConfig.Config.RuntimeParams, "pool_max_conns")
}

func TestConnectCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool, err := pool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	assert.Nil(t, pool)
	assert.Equal(t, context.Canceled, err)
}

func TestPoolAcquireAndConnRelease(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
}

func TestPoolExec(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testExec(t, pool)
}

func TestPoolQuery(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	// Test common usage
	testQuery(t, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior
	rows, err := pool.Query(context.Background(), "select generate_series(1,$1)", 10)
	require.NoError(t, err)

	stats := pool.Stat()
	assert.EqualValues(t, 1, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

	rows.Close()
	assert.NoError(t, rows.Err())
	waitForReleaseToComplete()

	stats = pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())

}

func TestPoolQueryRow(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testQueryRow(t, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())
}

func TestConnReleaseRollsBackFailedTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().PID()

	assert.Equal(t, byte('I'), c.Conn().TxStatus())

	_, err = c.Exec(ctx, "begin")
	assert.NoError(t, err)

	assert.Equal(t, byte('T'), c.Conn().TxStatus())

	_, err = c.Exec(ctx, "selct")
	assert.Error(t, err)

	assert.Equal(t, byte('E'), c.Conn().TxStatus())

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.Equal(t, pid, c.Conn().PID())
	assert.Equal(t, byte('I'), c.Conn().TxStatus())

	c.Release()
}

func TestConnReleaseRollsBackInTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().PID()

	assert.Equal(t, byte('I'), c.Conn().TxStatus())

	_, err = c.Exec(ctx, "begin")
	assert.NoError(t, err)

	assert.Equal(t, byte('T'), c.Conn().TxStatus())

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.Equal(t, pid, c.Conn().PID())
	assert.Equal(t, byte('I'), c.Conn().TxStatus())

	c.Release()
}

func TestConnReleaseDestroysClosedConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	c.Conn().Close(ctx)

	assert.EqualValues(t, 1, pool.Stat().TotalConns())

	c.Release()
	waitForReleaseToComplete()

	assert.EqualValues(t, 0, pool.Stat().TotalConns())
}

func TestConnPoolQueryConcurrentLoad(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	n := 100
	done := make(chan bool)

	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- true }()
			testQuery(t, pool)
			testQueryRow(t, pool)
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}
}
