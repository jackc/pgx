package pgx_test

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	"sync"
	"testing"
)

func createConnPool(t *testing.T, maxConnections int) *pgx.ConnPool {
	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: maxConnections}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	return pool
}

func TestNewConnPool(t *testing.T) {
	t.Parallel()

	var numCallbacks int
	afterConnect := func(c *pgx.Conn) error {
		numCallbacks++
		return nil
	}

	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 2, AfterConnect: afterConnect}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		t.Fatal("Unable to establish connection pool")
	}
	defer pool.Close()

	// It initially connects once
	stat := pool.Stat()
	if stat.CurrentConnections != 1 {
		t.Errorf("Expected 1 connection to be established immediately, but %v were", numCallbacks)
	}

	// Pool creation returns an error if any AfterConnect callback does
	errAfterConnect := errors.New("Some error")
	afterConnect = func(c *pgx.Conn) error {
		return errAfterConnect
	}

	config = pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 2, AfterConnect: afterConnect}
	pool, err = pgx.NewConnPool(config)
	if err != errAfterConnect {
		t.Errorf("Expected errAfterConnect but received unexpected: %v", err)
	}
}

func TestNewConnPoolDefaultsTo5MaxConnections(t *testing.T) {
	t.Parallel()

	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		t.Fatal("Unable to establish connection pool")
	}
	defer pool.Close()

	if n := pool.Stat().MaxConnections; n != 5 {
		t.Fatalf("Expected pool to default to 5 max connections, but it was %d", n)
	}
}

func TestNewConnPoolMaxConnectionsCannotBeLessThan2(t *testing.T) {
	t.Parallel()

	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 1}
	pool, err := pgx.NewConnPool(config)
	if err == nil {
		pool.Close()
		t.Fatal(`Expected NewConnPool to fail with "MaxConnections must be at least 2" error, but it succeeded`)
	}
	if err.Error() != "MaxConnections must be at least 2" {
		t.Fatalf(`Expected NewConnPool to fail with "MaxConnections must be at least 2" error, but it failed with %v`, err)
	}
}

func TestPoolAcquireAndReleaseCycle(t *testing.T) {
	t.Parallel()

	maxConnections := 2
	incrementCount := int32(100)
	completeSync := make(chan int)
	pool := createConnPool(t, maxConnections)
	defer pool.Close()

	acquireAll := func() (connections []*pgx.Conn) {
		connections = make([]*pgx.Conn, maxConnections)
		for i := 0; i < maxConnections; i++ {
			var err error
			if connections[i], err = pool.Acquire(); err != nil {
				t.Fatalf("Unable to acquire connection: %v", err)
			}
		}
		return
	}

	allConnections := acquireAll()

	for _, c := range allConnections {
		mustExec(t, c, "create temporary table t(counter integer not null)")
		mustExec(t, c, "insert into t(counter) values(0);")
	}

	for _, c := range allConnections {
		pool.Release(c)
	}

	f := func() {
		conn, err := pool.Acquire()
		if err != nil {
			t.Fatal("Unable to acquire connection")
		}
		defer pool.Release(conn)

		// Increment counter...
		mustExec(t, conn, "update t set counter = counter + 1")
		completeSync <- 0
	}

	for i := int32(0); i < incrementCount; i++ {
		go f()
	}

	// Wait for all f() to complete
	for i := int32(0); i < incrementCount; i++ {
		<-completeSync
	}

	// Check that temp table in each connection has been incremented some number of times
	actualCount := int32(0)
	allConnections = acquireAll()

	for _, c := range allConnections {
		var n int32
		c.QueryRow("select counter from t").Scan(&n)
		if n == 0 {
			t.Error("A connection was never used")
		}

		actualCount += n
	}

	if actualCount != incrementCount {
		fmt.Println(actualCount)
		t.Error("Wrong number of increments")
	}

	for _, c := range allConnections {
		pool.Release(c)
	}
}

func TestPoolReleaseWithTransactions(t *testing.T) {
	t.Parallel()

	pool := createConnPool(t, 2)
	defer pool.Close()

	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Unable to acquire connection: %v", err)
	}
	mustExec(t, conn, "begin")
	if _, err = conn.Exec("select"); err == nil {
		t.Fatal("Did not receive expected error")
	}
	if conn.TxStatus != 'E' {
		t.Fatalf("Expected TxStatus to be 'E', instead it was '%c'", conn.TxStatus)
	}

	pool.Release(conn)

	if conn.TxStatus != 'I' {
		t.Fatalf("Expected release to rollback errored transaction, but it did not: '%c'", conn.TxStatus)
	}

	conn, err = pool.Acquire()
	if err != nil {
		t.Fatalf("Unable to acquire connection: %v", err)
	}
	mustExec(t, conn, "begin")
	if conn.TxStatus != 'T' {
		t.Fatalf("Expected txStatus to be 'T', instead it was '%c'", conn.TxStatus)
	}

	pool.Release(conn)

	if conn.TxStatus != 'I' {
		t.Fatalf("Expected release to rollback uncommitted transaction, but it did not: '%c'", conn.TxStatus)
	}
}

func TestPoolAcquireAndReleaseCycleAutoConnect(t *testing.T) {
	t.Parallel()

	maxConnections := 3
	pool := createConnPool(t, maxConnections)
	defer pool.Close()

	doSomething := func() {
		c, err := pool.Acquire()
		if err != nil {
			t.Fatalf("Unable to Acquire: %v", err)
		}
		rows, _ := c.Query("select 1")
		rows.Close()
		pool.Release(c)
	}

	for i := 0; i < 1000; i++ {
		doSomething()
	}

	stat := pool.Stat()
	if stat.CurrentConnections != 1 {
		t.Fatalf("Pool shouldn't have established more connections when no contention: %v", stat.CurrentConnections)
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doSomething()
		}()
	}
	wg.Wait()

	stat = pool.Stat()
	if stat.CurrentConnections != stat.MaxConnections {
		t.Fatalf("Pool should have used all possible connections: %v", stat.CurrentConnections)
	}
}

func TestPoolReleaseDiscardsDeadConnections(t *testing.T) {
	t.Parallel()

	maxConnections := 3
	pool := createConnPool(t, maxConnections)
	defer pool.Close()

	var c1, c2 *pgx.Conn
	var err error
	var stat pgx.ConnPoolStat

	if c1, err = pool.Acquire(); err != nil {
		t.Fatalf("Unexpected error acquiring connection: %v", err)
	}
	defer func() {
		if c1 != nil {
			pool.Release(c1)
		}
	}()

	if c2, err = pool.Acquire(); err != nil {
		t.Fatalf("Unexpected error acquiring connection: %v", err)
	}
	defer func() {
		if c2 != nil {
			pool.Release(c2)
		}
	}()

	if _, err = c2.Exec("select pg_terminate_backend($1)", c1.Pid); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	// do something with the connection so it knows it's dead
	rows, _ := c1.Query("select 1")
	rows.Close()
	if rows.Err() == nil {
		t.Fatal("Expected error but none occurred")
	}

	if c1.IsAlive() {
		t.Fatal("Expected connection to be dead but it wasn't")
	}

	stat = pool.Stat()
	if stat.CurrentConnections != 2 {
		t.Fatalf("Unexpected CurrentConnections: %v", stat.CurrentConnections)
	}
	if stat.AvailableConnections != 0 {
		t.Fatalf("Unexpected AvailableConnections: %v", stat.CurrentConnections)
	}

	pool.Release(c1)
	c1 = nil // so it doesn't get released again by the defer

	stat = pool.Stat()
	if stat.CurrentConnections != 1 {
		t.Fatalf("Unexpected CurrentConnections: %v", stat.CurrentConnections)
	}
	if stat.AvailableConnections != 0 {
		t.Fatalf("Unexpected AvailableConnections: %v", stat.CurrentConnections)
	}
}

func TestConnPoolTransaction(t *testing.T) {
	t.Parallel()

	pool := createConnPool(t, 2)
	defer pool.Close()

	stats := pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}

	tx, err := pool.Begin()
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer tx.Rollback()

	var n int32
	err = tx.QueryRow("select 40+$1", 2).Scan(&n)
	if err != nil {
		t.Fatalf("tx.QueryRow Scan failed: %v", err)
	}
	if n != 42 {
		t.Errorf("Expected 42, got %d", n)
	}

	stats = pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 0 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("tx.Rollback failed: %v", err)
	}

	stats = pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}

func TestConnPoolTransactionIso(t *testing.T) {
	t.Parallel()

	pool := createConnPool(t, 2)
	defer pool.Close()

	tx, err := pool.BeginIso(pgx.Serializable)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer tx.Rollback()

	var level string
	err = tx.QueryRow("select current_setting('transaction_isolation')").Scan(&level)
	if err != nil {
		t.Fatalf("tx.QueryRow failed: %v", level)
	}

	if level != "serializable" {
		t.Errorf("Expected to be in isolation level %v but was %v", "serializable", level)
	}
}

func TestConnPoolQuery(t *testing.T) {
	t.Parallel()

	pool := createConnPool(t, 2)
	defer pool.Close()

	var sum, rowCount int32

	rows, err := pool.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("pool.Query failed: %v", err)
	}

	stats := pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 0 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}

	stats = pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}

func TestConnPoolQueryRow(t *testing.T) {
	t.Parallel()

	pool := createConnPool(t, 2)
	defer pool.Close()

	var n int32
	err := pool.QueryRow("select 40+$1", 2).Scan(&n)
	if err != nil {
		t.Fatalf("pool.QueryRow Scan failed: %v", err)
	}

	if n != 42 {
		t.Errorf("Expected 42, got %d", n)
	}

	stats := pool.Stat()
	if stats.CurrentConnections != 1 || stats.AvailableConnections != 1 {
		t.Fatalf("Unexpected connection pool stats: %v", stats)
	}
}
