package pgxpool_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	t.Parallel()
	connString := os.Getenv("PGX_TEST_DATABASE")
	pool, err := pgxpool.Connect(context.Background(), connString)
	require.NoError(t, err)
	assert.Equal(t, connString, pool.Config().ConnString())
	pool.Close()
}

func TestConnectConfig(t *testing.T) {
	t.Parallel()
	connString := os.Getenv("PGX_TEST_DATABASE")
	config, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
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

func TestConnectCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool, err := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	assert.Nil(t, pool)
	assert.Equal(t, context.Canceled, err)
}

func TestLazyConnect(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	assert.NoError(t, err)
	config.LazyConnect = true

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool, err := pgxpool.ConnectConfig(ctx, config)
	assert.NoError(t, err)

	_, err = pool.Exec(ctx, "SELECT 1")
	assert.Equal(t, context.Canceled, err)
}

func TestConnectConfigRequiresConnConfigFromParseConfig(t *testing.T) {
	t.Parallel()

	config := &pgxpool.Config{}

	require.PanicsWithValue(t, "config must be created by ParseConfig", func() { pgxpool.ConnectConfig(context.Background(), config) })
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
		_, err = pgxpool.ConnectConfig(context.Background(), copied)
	})
	assert.NoError(t, err)
}

func TestPoolAcquireAndConnRelease(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
}

func TestPoolAcquireFunc(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	var n int32
	err = pool.AcquireFunc(context.Background(), func(c *pgxpool.Conn) error {
		return c.QueryRow(context.Background(), "select 1").Scan(&n)
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestPoolAcquireFuncReturnsFnError(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	err = pool.AcquireFunc(context.Background(), func(c *pgxpool.Conn) error {
		return fmt.Errorf("some error")
	})
	require.EqualError(t, err, "some error")
}

func TestPoolBeforeConnect(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
		cfg.Config.RuntimeParams["application_name"] = "pgx"
		return nil
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	var str string
	err = db.QueryRow(context.Background(), "SHOW application_name").Scan(&str)
	require.NoError(t, err)
	assert.EqualValues(t, "pgx", str)
}

func TestPoolAfterConnect(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Prepare(ctx, "ps1", "select 1")
		return err
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	var n int32
	err = db.QueryRow(context.Background(), "ps1").Scan(&n)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

func TestPoolBeforeAcquire(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	acquireAttempts := 0

	config.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
		acquireAttempts++
		return acquireAttempts%2 == 0
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	conns := make([]*pgxpool.Conn, 4)
	for i := range conns {
		conns[i], err = db.Acquire(context.Background())
		assert.NoError(t, err)
	}

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	assert.EqualValues(t, 8, acquireAttempts)

	conns = db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 2)

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	assert.EqualValues(t, 12, acquireAttempts)
}

func TestPoolAfterRelease(t *testing.T) {
	t.Parallel()

	func() {
		pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		defer pool.Close()

		err = pool.AcquireFunc(context.Background(), func(conn *pgxpool.Conn) error {
			if conn.Conn().PgConn().ParameterStatus("crdb_version") != "" {
				t.Skip("Server does not support backend PID")
			}
			return nil
		})
		require.NoError(t, err)
	}()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	afterReleaseCount := 0

	config.AfterRelease = func(c *pgx.Conn) bool {
		afterReleaseCount++
		return afterReleaseCount%2 == 1
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	connPIDs := map[uint32]struct{}{}

	for i := 0; i < 10; i++ {
		conn, err := db.Acquire(context.Background())
		assert.NoError(t, err)
		connPIDs[conn.Conn().PgConn().PID()] = struct{}{}
		conn.Release()
		waitForReleaseToComplete()
	}

	assert.EqualValues(t, 5, len(connPIDs))
}

func TestPoolAcquireAllIdle(t *testing.T) {
	t.Parallel()

	db, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	conns := db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 1)

	for _, c := range conns {
		c.Release()
	}
	waitForReleaseToComplete()

	conns = make([]*pgxpool.Conn, 3)
	for i := range conns {
		conns[i], err = db.Acquire(context.Background())
		assert.NoError(t, err)
	}

	for _, c := range conns {
		if c != nil {
			c.Release()
		}
	}
	waitForReleaseToComplete()

	conns = db.AcquireAllIdle(context.Background())
	assert.Len(t, conns, 3)

	for _, c := range conns {
		c.Release()
	}
}

func TestConnReleaseChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 250 * time.Millisecond

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)

	time.Sleep(config.MaxConnLifetime)

	c.Release()
	waitForReleaseToComplete()

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestConnReleaseClosesBusyConn(t *testing.T) {
	t.Parallel()

	db, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)

	_, err = c.Query(context.Background(), "select generate_series(1,10)")
	require.NoError(t, err)

	c.Release()
	waitForReleaseToComplete()

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMaxConnLifetime(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 100 * time.Millisecond
	config.HealthCheckPeriod = 100 * time.Millisecond

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
	time.Sleep(config.MaxConnLifetime + 100*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMaxConnIdleTime(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.MaxConnLifetime = 1 * time.Minute
	config.MaxConnIdleTime = 100 * time.Millisecond
	config.HealthCheckPeriod = 150 * time.Millisecond

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	c, err := db.Acquire(context.Background())
	require.NoError(t, err)
	c.Release()
	time.Sleep(config.HealthCheckPeriod + 50*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 0, stats.TotalConns())
}

func TestPoolBackgroundChecksMinConns(t *testing.T) {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	config.HealthCheckPeriod = 100 * time.Millisecond
	config.MinConns = 2

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(t, err)
	defer db.Close()

	time.Sleep(config.HealthCheckPeriod + 100*time.Millisecond)

	stats := db.Stat()
	assert.EqualValues(t, 2, stats.TotalConns())
}

func TestPoolExec(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testExec(t, pool)
}

func TestPoolQuery(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
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
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	testQueryRow(t, pool)
	waitForReleaseToComplete()

	stats := pool.Stat()
	assert.EqualValues(t, 0, stats.AcquiredConns())
	assert.EqualValues(t, 1, stats.TotalConns())
}

// https://github.com/jackc/pgx/issues/677
func TestPoolQueryRowErrNoRows(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	err = pool.QueryRow(context.Background(), "select n from generate_series(1,10) n where n=0").Scan(nil)
	require.Equal(t, pgx.ErrNoRows, err)
}

func TestPoolSendBatch(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
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
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
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

func TestConnReleaseClosesConnInFailedTransaction(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	err = pool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
		if conn.Conn().PgConn().ParameterStatus("crdb_version") != "" {
			t.Skip("Server does not support backend PID")
		}
		return nil
	})
	require.NoError(t, err)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	err = pool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
		if conn.Conn().PgConn().ParameterStatus("crdb_version") != "" {
			t.Skip("Server does not support backend PID")
		}
		return nil
	})
	require.NoError(t, err)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer pool.Close()

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)

	err = c.Conn().Close(ctx)
	require.NoError(t, err)

	assert.EqualValues(t, 1, pool.Stat().TotalConns())

	c.Release()
	waitForReleaseToComplete()

	assert.EqualValues(t, 0, pool.Stat().TotalConns())
}

func TestConnPoolQueryConcurrentLoad(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
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

func TestConnReleaseWhenBeginFail(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := pgxpool.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
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
	db, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	createSql := `
		drop table if exists pgxpooltx;
    create temporary table pgxpooltx(
      id integer,
      unique (id)
    );
  `

	_, err = db.Exec(context.Background(), createSql)
	require.NoError(t, err)

	defer func() {
		db.Exec(context.Background(), "drop table pgxpooltx")
	}()

	err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
		_, err := db.Exec(context.Background(), "insert into pgxpooltx(id) values (1)")
		require.NoError(t, err)

		err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
			_, err := db.Exec(context.Background(), "insert into pgxpooltx(id) values (2)")
			require.NoError(t, err)

			err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
				_, err := db.Exec(context.Background(), "insert into pgxpooltx(id) values (3)")
				require.NoError(t, err)
				return nil
			})

			return nil
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	var n int64
	err = db.QueryRow(context.Background(), "select count(*) from pgxpooltx").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
}

func TestTxBeginFuncNestedTransactionRollback(t *testing.T) {
	db, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer db.Close()

	createSql := `
		drop table if exists pgxpooltx;
    create temporary table pgxpooltx(
      id integer,
      unique (id)
    );
  `

	_, err = db.Exec(context.Background(), createSql)
	require.NoError(t, err)

	defer func() {
		db.Exec(context.Background(), "drop table pgxpooltx")
	}()

	err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
		_, err := db.Exec(context.Background(), "insert into pgxpooltx(id) values (1)")
		require.NoError(t, err)

		err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
			_, err := db.Exec(context.Background(), "insert into pgxpooltx(id) values (2)")
			require.NoError(t, err)
			return errors.New("do a rollback")
		})
		require.EqualError(t, err, "do a rollback")

		_, err = db.Exec(context.Background(), "insert into pgxpooltx(id) values (3)")
		require.NoError(t, err)

		return nil
	})

	var n int64
	err = db.QueryRow(context.Background(), "select count(*) from pgxpooltx").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
}

func TestIdempotentPoolClose(t *testing.T) {
	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	// Close the open pool.
	require.NotPanics(t, func() { pool.Close() })

	// Close the already closed pool.
	require.NotPanics(t, func() { pool.Close() })
}
