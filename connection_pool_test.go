package pgx_test

import (
	"errors"
	"fmt"
	"github.com/JackC/pgx"
	"sync"
	"testing"
)

func createConnectionPool(t *testing.T, maxConnections int) *pgx.ConnectionPool {
	options := pgx.ConnectionPoolOptions{MaxConnections: maxConnections}
	pool, err := pgx.NewConnectionPool(*defaultConnectionParameters, options)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	return pool
}

func TestNewConnectionPool(t *testing.T) {
	var numCallbacks int
	afterConnect := func(c *pgx.Connection) error {
		numCallbacks++
		return nil
	}

	options := pgx.ConnectionPoolOptions{MaxConnections: 2, AfterConnect: afterConnect}
	pool, err := pgx.NewConnectionPool(*defaultConnectionParameters, options)
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
	afterConnect = func(c *pgx.Connection) error {
		return errAfterConnect
	}

	options = pgx.ConnectionPoolOptions{MaxConnections: 2, AfterConnect: afterConnect}
	pool, err = pgx.NewConnectionPool(*defaultConnectionParameters, options)
	if err != errAfterConnect {
		t.Errorf("Expected errAfterConnect but received unexpected: %v", err)
	}
}

func TestPoolAcquireAndReleaseCycle(t *testing.T) {
	maxConnections := 2
	incrementCount := int32(100)
	completeSync := make(chan int)
	pool := createConnectionPool(t, maxConnections)
	defer pool.Close()

	acquireAll := func() (connections []*pgx.Connection) {
		connections = make([]*pgx.Connection, maxConnections)
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
		mustExecute(t, c, "create temporary table t(counter integer not null)")
		mustExecute(t, c, "insert into t(counter) values(0);")
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
		mustExecute(t, conn, "update t set counter = counter + 1")
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
		v := mustSelectValue(t, c, "select counter from t")
		n := v.(int32)
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
	pool := createConnectionPool(t, 1)
	defer pool.Close()

	conn, err := pool.Acquire()
	if err != nil {
		t.Fatalf("Unable to acquire connection: %v", err)
	}
	mustExecute(t, conn, "begin")
	if _, err = conn.Execute("select"); err == nil {
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
	mustExecute(t, conn, "begin")
	if conn.TxStatus != 'T' {
		t.Fatalf("Expected txStatus to be 'T', instead it was '%c'", conn.TxStatus)
	}

	pool.Release(conn)

	if conn.TxStatus != 'I' {
		t.Fatalf("Expected release to rollback uncommitted transaction, but it did not: '%c'", conn.TxStatus)
	}
}

func TestPoolAcquireAndReleaseCycleAutoConnect(t *testing.T) {
	maxConnections := 3
	pool := createConnectionPool(t, maxConnections)
	defer pool.Close()

	doSomething := func() {
		c, err := pool.Acquire()
		if err != nil {
			t.Fatalf("Unable to Acquire: %v", err)
		}
		c.SelectValue("select 1")
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
