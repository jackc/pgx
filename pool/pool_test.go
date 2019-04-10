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
	rows, err := pool.Query("select generate_series(1,$1)", 10)
	require.NoError(t, err)

	stats := pool.Stat()
	assert.Equal(t, 1, stats.AcquiredConns())
	assert.Equal(t, 1, stats.TotalConns())

	rows.Close()
	assert.NoError(t, rows.Err())
	waitForReleaseToComplete()

	stats = pool.Stat()
	assert.Equal(t, 0, stats.AcquiredConns())
	assert.Equal(t, 1, stats.TotalConns())

}

func TestPoolQueryEx(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	// Test common usage
	testQueryEx(t, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior

	rows, err := pool.QueryEx(context.Background(), "select generate_series(1,$1)", nil, 10)
	require.NoError(t, err)

	stats := pool.Stat()
	assert.Equal(t, 1, stats.AcquiredConns())
	assert.Equal(t, 1, stats.TotalConns())

	rows.Close()
	assert.NoError(t, rows.Err())
	waitForReleaseToComplete()

	stats = pool.Stat()
	assert.Equal(t, 0, stats.AcquiredConns())
	assert.Equal(t, 1, stats.TotalConns())
}

func TestPoolQueryRow(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testQueryRow(t, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.Equal(t, 0, stats.AcquiredConns())
	assert.Equal(t, 1, stats.TotalConns())
}

func TestPoolQueryRowEx(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testQueryRowEx(t, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.Equal(t, 0, stats.AcquiredConns())
	assert.Equal(t, 1, stats.TotalConns())
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

	c.Conn().Close()

	assert.Equal(t, 1, pool.Stat().TotalConns())

	c.Release()
	waitForReleaseToComplete()

	assert.Equal(t, 0, pool.Stat().TotalConns())
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
			testQueryEx(t, pool)
			testQueryRow(t, pool)
			testQueryRowEx(t, pool)
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}
}
