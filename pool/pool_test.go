package pool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pool"
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

func TestPoolSendBatch(t *testing.T) {
	pool, err := pool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testSendBatch(t, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())
}

func TestPoolCopyFrom(t *testing.T) {
	// Not able to use testCopyFrom because it relies on temporary tables and the pool may run subsequent calls under
	// different connections.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `drop table if exists poolcopyfromtest`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `create table poolcopyfromtest(a int2, b int4, c int8, d varchar, e text, f date, g timestamptz)`)
	require.NoError(t, err)
	defer pool.Exec(ctx, `drop table poolcopyfromtest`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]interface{}{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := pool.CopyFrom(ctx, pgx.Identifier{"poolcopyfromtest"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
	assert.NoError(t, err)
	assert.EqualValues(t, len(inputRows), copyCount)

	rows, err := pool.Query(ctx, "select * from poolcopyfromtest")
	assert.NoError(t, err)

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	assert.NoError(t, rows.Err())
	assert.Equal(t, inputRows, outputRows)
}

func TestConnReleaseRollsBackFailedTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().PgConn().PID()

	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus)

	_, err = c.Exec(ctx, "begin")
	assert.NoError(t, err)

	assert.Equal(t, byte('T'), c.Conn().PgConn().TxStatus)

	_, err = c.Exec(ctx, "selct")
	assert.Error(t, err)

	assert.Equal(t, byte('E'), c.Conn().PgConn().TxStatus)

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.Equal(t, pid, c.Conn().PgConn().PID())
	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus)

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

	pid := c.Conn().PgConn().PID()

	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus)

	_, err = c.Exec(ctx, "begin")
	assert.NoError(t, err)

	assert.Equal(t, byte('T'), c.Conn().PgConn().TxStatus)

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.Equal(t, pid, c.Conn().PgConn().PID())
	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus)

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
