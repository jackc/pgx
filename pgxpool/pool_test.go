package pgxpool_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	connString := os.Getenv("PGX_TEST_DATABASE")
	pool, err := pgxpool.New(ctx, connString)
	require.NoError(t, err)
	assert.Equal(t, connString, pool.Config().ConnString())
	pool.Close()
}

func TestConnectConfig(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	connString := os.Getenv("PGX_TEST_DATABASE")
	config, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	assertConfigsEqual(t, config, pool.Config(), "Pool.Config() returns original config")
	pool.Close()
}

func TestParseConfigExtractsPoolArguments(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig("pool_max_conns=42 pool_min_conns=1")
	assert.NoError(t, err)
	assert.EqualValues(t, 42, config.MaxConns)
	assert.EqualValues(t, 1, config.MinConns)
	assert.NotContains(t, config.ConnConfig.Config.RuntimeParams, "pool_max_conns")
	assert.NotContains(t, config.ConnConfig.Config.RuntimeParams, "pool_min_conns")
}

func TestConstructorIgnoresContext(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	assert.NoError(t, err)
	var cancel func()
	config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		// cancel the query's context before we actually Dial to ensure the Dial's
		// context isn't cancelled
		cancel()
		return nil
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)

	assert.EqualValues(t, 0, pool.Stat().TotalConns())

	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	_, err = pool.Exec(ctx, "SELECT 1")
	assert.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 1, pool.Stat().TotalConns())
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &pgxpool.Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { pgxpool.NewWithConfig(context.Background(), config) })
}

func TestConfigCopyReturnsEqualConfig(t *testing.T) {
	connString := "postgres://jack:secret@localhost:5432/mydb?application_name=pgxtest&search_path=myschema&connect_timeout=5"
	original, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()

	assertConfigsEqual(t, original, copied, t.Name())
}

func TestConfigCopyCanBeUsedToConnect(t *testing.T) {
	connString := os.Getenv("PGX_TEST_DATABASE")
	original, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assert.NotPanics(t, func() {
		_, err = pgxpool.NewWithConfig(context.Background(), copied)
	})
	assert.NoError(t, err)
}

func TestPoolAcquireAndConnRelease(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	c.Release()
}

func TestPoolAcquireAndConnHijack(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	connsBeforeHijack := pool.Stat().TotalConns()

	conn := c.Hijack()
	defer conn.Close(ctx)

	connsAfterHijack := pool.Stat().TotalConns()
	require.Equal(t, connsBeforeHijack-1, connsAfterHijack)

	var n int32
	err = conn.QueryRow(ctx, `select 1`).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, int32(1), n)
}

func TestPoolAcquireChecksIdleConns(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	controllerConn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer controllerConn.Close(ctx)
	pgxtest.SkipCockroachDB(t, controllerConn, "Server does not support pg_terminate_backend() (https://github.com/cockroachdb/cockroach/issues/35897)")

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	var conns []*pgxpool.Conn
	for i := 0; i < 3; i++ {
		c, err := pool.Acquire(ctx)
		require.NoError(t, err)
		conns = append(conns, c)
	}

	require.EqualValues(t, 3, pool.Stat().TotalConns())

	var pids []uint32
	for _, c := range conns {
		pids = append(pids, c.Conn().PgConn().PID())
		c.Release()
	}

	_, err = controllerConn.Exec(ctx, `select pg_terminate_backend(n) from unnest($1::int[]) n`, pids)
	require.NoError(t, err)

	// All conns are dead they don't know it and neither does the pool.
	require.EqualValues(t, 3, pool.Stat().TotalConns())

	// Wait long enough so the pool will realize it needs to check the connections.
	time.Sleep(time.Second)

	// Pool should try all existing connections and find them dead, then create a new connection which should successfully ping.
	err = pool.Ping(ctx)
	require.NoError(t, err)

	// The original 3 conns should have been terminated and the a new conn established for the ping.
	require.EqualValues(t, 1, pool.Stat().TotalConns())
	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	cPID := c.Conn().PgConn().PID()
	c.Release()

	require.NotContains(t, pids, cPID)
}

func TestPoolAcquireFunc(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	var n int32
	err = pool.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		return c.QueryRow(ctx, "select 1").Scan(&n)
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestPoolAcquireFuncReturnsFnError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	err = pool.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		return fmt.Errorf("some error")
	})
	require.EqualError(t, err, "some error")
}

func TestPoolBeforeConnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
		cfg.Config.RuntimeParams["application_name"] = "pgx"
		return nil
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	var str string
	err = db.QueryRow(ctx, "SHOW application_name").Scan(&str)
	require.NoError(t, err)
	assert.EqualValues(t, "pgx", str)
}

func TestPoolAfterConnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Prepare(ctx, "ps1", "select 1")
		return err
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	var n int32
	err = db.QueryRow(ctx, "ps1").Scan(&n)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

func TestPoolBeforeAcquire(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	acquireAttempts := 0

	config.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
		acquireAttempts++
		return acquireAttempts%2 == 0
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	conns := make([]*pgxpool.Conn, 4)
	for i := range conns {
		conns[i], err = db.Acquire(ctx)
		assert.NoError(t, err)
	}

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	assert.EqualValues(t, 8, acquireAttempts)

	conns = db.AcquireAllIdle(ctx)
	assert.Len(t, conns, 2)

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	assert.EqualValues(t, 12, acquireAttempts)
}

func TestPoolAfterRelease(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	func() {
		pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		defer pool.Close()
	}()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	afterReleaseCount := 0

	config.AfterRelease = func(c *pgx.Conn) bool {
		afterReleaseCount++
		return afterReleaseCount%2 == 1
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	connPIDs := map[uint32]struct{}{}

	for i := 0; i < 10; i++ {
		conn, err := db.Acquire(ctx)
		assert.NoError(t, err)
		connPIDs[conn.Conn().PgConn().PID()] = struct{}{}
		conn.Release()
		waitForReleaseToComplete()
	}

	assert.EqualValues(t, 5, len(connPIDs))
}

func TestPoolBeforeClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	func() {
		pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		defer pool.Close()
	}()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	connPIDs := make(chan uint32, 5)
	config.BeforeClose = func(c *pgx.Conn) {
		connPIDs <- c.PgConn().PID()
	}

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	acquiredPIDs := make([]uint32, 0, 5)
	closedPIDs := make([]uint32, 0, 5)
	for i := 0; i < 5; i++ {
		conn, err := db.Acquire(ctx)
		assert.NoError(t, err)
		acquiredPIDs = append(acquiredPIDs, conn.Conn().PgConn().PID())
		conn.Release()
		db.Reset()
		closedPIDs = append(closedPIDs, <-connPIDs)
	}

	assert.ElementsMatch(t, acquiredPIDs, closedPIDs)
}

func TestPoolAcquireAllIdle(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	conns := make([]*pgxpool.Conn, 3)
	for i := range conns {
		conns[i], err = db.Acquire(ctx)
		assert.NoError(t, err)
	}

	for _, c := range conns {
		if c != nil {
			c.Release()
		}
	}
	waitForReleaseToComplete()

	conns = db.AcquireAllIdle(ctx)
	assert.Len(t, conns, 3)

	for _, c := range conns {
		c.Release()
	}
}

func TestPoolReset(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	conns := make([]*pgxpool.Conn, 3)
	for i := range conns {
		conns[i], err = db.Acquire(ctx)
		assert.NoError(t, err)
	}

	db.Reset()

	for _, c := range conns {
		if c != nil {
			c.Release()
		}
	}
	waitForReleaseToComplete()

	require.EqualValues(t, 0, db.Stat().TotalConns())
}

func TestConnReleaseChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 250 * time.Millisecond

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(ctx)
	require.NoError(t, err)

	time.Sleep(config.MaxConnLifetime)

	c.Release()
	waitForReleaseToComplete()

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestConnReleaseClosesBusyConn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(ctx)
	require.NoError(t, err)

	_, err = c.Query(ctx, "select generate_series(1,10)")
	require.NoError(t, err)

	c.Release()
	waitForReleaseToComplete()

	// wait for the connection to actually be destroyed
	for i := 0; i < 1000; i++ {
		if db.Stat().TotalConns() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 100 * time.Millisecond
	config.HealthCheckPeriod = 100 * time.Millisecond

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(ctx)
	require.NoError(t, err)
	c.Release()
	time.Sleep(config.MaxConnLifetime + 500*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
	assert.EqualValues(t, 0, stats.MaxIdleDestroyCount())
	assert.EqualValues(t, 1, stats.MaxLifetimeDestroyCount())
	assert.EqualValues(t, 1, stats.NewConnsCount())
}

func TestPoolBackgroundChecksMaxConnIdleTime(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 1 * time.Minute
	config.MaxConnIdleTime = 100 * time.Millisecond
	config.HealthCheckPeriod = 150 * time.Millisecond

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(ctx)
	require.NoError(t, err)
	c.Release()
	time.Sleep(config.HealthCheckPeriod)

	for i := 0; i < 1000; i++ {
		if db.Stat().TotalConns() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
	assert.EqualValues(t, 1, stats.MaxIdleDestroyCount())
	assert.EqualValues(t, 0, stats.MaxLifetimeDestroyCount())
	assert.EqualValues(t, 1, stats.NewConnsCount())
}

func TestPoolBackgroundChecksMinConns(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.HealthCheckPeriod = 100 * time.Millisecond
	config.MinConns = 2

	db, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	stats := db.Stat()
	for !(stats.IdleConns() == 2 && stats.MaxLifetimeDestroyCount() == 0 && stats.NewConnsCount() == 2) && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
		stats = db.Stat()
	}
	require.EqualValues(t, 2, stats.IdleConns())
	require.EqualValues(t, 0, stats.MaxLifetimeDestroyCount())
	require.EqualValues(t, 2, stats.NewConnsCount())

	c, err := db.Acquire(ctx)
	require.NoError(t, err)

	stats = db.Stat()
	require.EqualValues(t, 1, stats.IdleConns())
	require.EqualValues(t, 0, stats.MaxLifetimeDestroyCount())
	require.EqualValues(t, 2, stats.NewConnsCount())

	err = c.Conn().Close(ctx)
	require.NoError(t, err)
	c.Release()

	stats = db.Stat()
	for !(stats.IdleConns() == 2 && stats.MaxIdleDestroyCount() == 0 && stats.NewConnsCount() == 3) && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
		stats = db.Stat()
	}
	require.EqualValues(t, 2, stats.TotalConns())
	require.EqualValues(t, 0, stats.MaxIdleDestroyCount())
	require.EqualValues(t, 3, stats.NewConnsCount())
}

func TestPoolExec(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testExec(t, ctx, pool)
}

func TestPoolQuery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	// Test common usage
	testQuery(t, ctx, pool)
	waitForReleaseToComplete()

	// Test expected pool behavior
	rows, err := pool.Query(ctx, "select generate_series(1,$1)", 10)
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
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testQueryRow(t, ctx, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())
}

// https://github.com/jackc/pgx/issues/677
func TestPoolQueryRowErrNoRows(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	err = pool.QueryRow(ctx, "select n from generate_series(1,10) n where n=0").Scan(nil)
	require.Equal(t, pgx.ErrNoRows, err)
}

// https://github.com/jackc/pgx/issues/1628
func TestPoolQueryRowScanPanicReleasesConnection(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	require.Panics(t, func() {
		var greeting *string
		pool.QueryRow(ctx, "select 'Hello, world!'").Scan(greeting) // Note lack of &. This means that a typed nil is passed to Scan.
	})

	// If the connection is not released this will block forever in the defer pool.Close().
}

func TestPoolSendBatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testSendBatch(t, ctx, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())
}

func TestPoolCopyFrom(t *testing.T) {
	// Not able to use testCopyFrom because it relies on temporary tables and the pool may run subsequent calls under
	// different connections.
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `drop table if exists poolcopyfromtest`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `create table poolcopyfromtest(a int2, b int4, c int8, d varchar, e text, f date, g timestamptz)`)
	require.NoError(t, err)
	defer pool.Exec(ctx, `drop table poolcopyfromtest`)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]any{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := pool.CopyFrom(ctx, pgx.Identifier{"poolcopyfromtest"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
	assert.NoError(t, err)
	assert.EqualValues(t, len(inputRows), copyCount)

	rows, err := pool.Query(ctx, "select * from poolcopyfromtest")
	assert.NoError(t, err)

	var outputRows [][]any
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

func TestConnReleaseClosesConnInFailedTransaction(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().PgConn().PID()

	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus())

	_, err = c.Exec(ctx, "begin")
	assert.NoError(t, err)

	assert.Equal(t, byte('T'), c.Conn().PgConn().TxStatus())

	_, err = c.Exec(ctx, "selct")
	assert.Error(t, err)

	assert.Equal(t, byte('E'), c.Conn().PgConn().TxStatus())

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.NotEqual(t, pid, c.Conn().PgConn().PID())
	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus())

	c.Release()
}

func TestConnReleaseClosesConnInTransaction(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	pid := c.Conn().PgConn().PID()

	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus())

	_, err = c.Exec(ctx, "begin")
	assert.NoError(t, err)

	assert.Equal(t, byte('T'), c.Conn().PgConn().TxStatus())

	c.Release()
	waitForReleaseToComplete()

	c, err = pool.Acquire(ctx)
	require.NoError(t, err)

	assert.NotEqual(t, pid, c.Conn().PgConn().PID())
	assert.Equal(t, byte('I'), c.Conn().PgConn().TxStatus())

	c.Release()
}

func TestConnReleaseDestroysClosedConn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	err = c.Conn().Close(ctx)
	require.NoError(t, err)

	assert.EqualValues(t, 1, pool.Stat().TotalConns())

	c.Release()
	waitForReleaseToComplete()

	// wait for the connection to actually be destroyed
	for i := 0; i < 1000; i++ {
		if pool.Stat().TotalConns() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	assert.EqualValues(t, 0, pool.Stat().TotalConns())
}

func TestConnPoolQueryConcurrentLoad(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	n := 100
	done := make(chan bool)

	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- true }()
			testQuery(t, ctx, pool)
			testQueryRow(t, ctx, pool)
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}
}

func TestConnReleaseWhenBeginFail(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.TxIsoLevel("foo"),
	})
	assert.Error(t, err)
	if !assert.Zero(t, tx) {
		err := tx.Rollback(ctx)
		assert.NoError(t, err)
	}

	for i := 0; i < 1000; i++ {
		if db.Stat().TotalConns() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	assert.EqualValues(t, 0, db.Stat().TotalConns())
}

func TestTxBeginFuncNestedTransactionCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	createSql := `
		drop table if exists pgxpooltx;
    create temporary table pgxpooltx(
      id integer,
      unique (id)
    );
  `

	_, err = db.Exec(ctx, createSql)
	require.NoError(t, err)

	defer func() {
		db.Exec(ctx, "drop table pgxpooltx")
	}()

	err = pgx.BeginFunc(ctx, db, func(db pgx.Tx) error {
		_, err := db.Exec(ctx, "insert into pgxpooltx(id) values (1)")
		require.NoError(t, err)

		err = pgx.BeginFunc(ctx, db, func(db pgx.Tx) error {
			_, err := db.Exec(ctx, "insert into pgxpooltx(id) values (2)")
			require.NoError(t, err)

			err = pgx.BeginFunc(ctx, db, func(db pgx.Tx) error {
				_, err := db.Exec(ctx, "insert into pgxpooltx(id) values (3)")
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	var n int64
	err = db.QueryRow(ctx, "select count(*) from pgxpooltx").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
}

func TestTxBeginFuncNestedTransactionRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	createSql := `
		drop table if exists pgxpooltx;
    create temporary table pgxpooltx(
      id integer,
      unique (id)
    );
  `

	_, err = db.Exec(ctx, createSql)
	require.NoError(t, err)

	defer func() {
		db.Exec(ctx, "drop table pgxpooltx")
	}()

	err = pgx.BeginFunc(ctx, db, func(db pgx.Tx) error {
		_, err := db.Exec(ctx, "insert into pgxpooltx(id) values (1)")
		require.NoError(t, err)

		err = pgx.BeginFunc(ctx, db, func(db pgx.Tx) error {
			_, err := db.Exec(ctx, "insert into pgxpooltx(id) values (2)")
			require.NoError(t, err)
			return errors.New("do a rollback")
		})
		require.EqualError(t, err, "do a rollback")

		_, err = db.Exec(ctx, "insert into pgxpooltx(id) values (3)")
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	var n int64
	err = db.QueryRow(ctx, "select count(*) from pgxpooltx").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
}

func TestIdempotentPoolClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	// Close the open pool.
	require.NotPanics(t, func() { pool.Close() })

	// Close the already closed pool.
	require.NotPanics(t, func() { pool.Close() })
}

func TestConnectEagerlyReachesMinPoolSize(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MinConns = int32(12)
	config.MaxConns = int32(15)

	acquireAttempts := int64(0)
	connectAttempts := int64(0)

	config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		atomic.AddInt64(&acquireAttempts, 1)
		return true
	}
	config.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
		atomic.AddInt64(&connectAttempts, 1)
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	for i := 0; i < 500; i++ {
		time.Sleep(10 * time.Millisecond)

		stat := pool.Stat()
		if stat.IdleConns() == 12 && stat.AcquireCount() == 0 && stat.TotalConns() == 12 && atomic.LoadInt64(&acquireAttempts) == 0 && atomic.LoadInt64(&connectAttempts) == 12 {
			return
		}
	}

	t.Fatal("did not reach min pool size")

}

func TestPoolSendBatchBatchCloseTwice(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	errChan := make(chan error)
	testCount := 5000

	for i := 0; i < testCount; i++ {
		go func() {
			batch := &pgx.Batch{}
			batch.Queue("select 1")
			batch.Queue("select 2")

			br := pool.SendBatch(ctx, batch)
			defer br.Close()

			var err error
			var n int32
			err = br.QueryRow().Scan(&n)
			if err != nil {
				errChan <- err
				return
			}
			if n != 1 {
				errChan <- fmt.Errorf("expected 1 got %v", n)
				return
			}

			err = br.QueryRow().Scan(&n)
			if err != nil {
				errChan <- err
				return
			}
			if n != 2 {
				errChan <- fmt.Errorf("expected 2 got %v", n)
				return
			}

			err = br.Close()
			errChan <- err
		}()
	}

	for i := 0; i < testCount; i++ {
		err := <-errChan
		assert.NoError(t, err)
	}
}
