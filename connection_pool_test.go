package pgx

import (
	"fmt"
	"testing"
)

func createConnectionPool(maxConnections int) *ConnectionPool {
	pool, err := NewConnectionPool(*defaultConnectionParameters, maxConnections)
	if err != nil {
		panic("Unable to create connection pool")
	}
	return pool
}

func TestNewConnectionPool(t *testing.T) {
	pool, err := NewConnectionPool(*defaultConnectionParameters, 5)
	if err != nil {
		t.Fatal("Unable to establish connection pool")
	}
	defer pool.Close()

	if pool.MaxConnections != 5 {
		t.Error("Wrong maxConnections")
	}
}

func TestPoolAcquireAndReleaseCycle(t *testing.T) {
	maxConnections := 2
	incrementCount := int32(100)
	completeSync := make(chan int)
	pool := createConnectionPool(maxConnections)
	defer pool.Close()

	acquireAll := func() (connections []*Connection) {
		connections = make([]*Connection, maxConnections)
		for i := 0; i < maxConnections; i++ {
			connections[i] = pool.Acquire()
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
		var err error
		conn := pool.Acquire()
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
	pool := createConnectionPool(1)
	defer pool.Close()

	var err error
	conn := pool.Acquire()
	mustExecute(t, conn, "begin")
	if _, err = conn.Execute("select"); err == nil {
		t.Fatal("Did not receive expected error")
	}
	if conn.txStatus != 'E' {
		t.Fatalf("Expected txStatus to be 'E', instead it was '%c'", conn.txStatus)
	}

	pool.Release(conn)

	if conn.txStatus != 'I' {
		t.Fatalf("Expected release to rollback errored transaction, but it did not: '%c'", conn.txStatus)
	}

	conn = pool.Acquire()
	mustExecute(t, conn, "begin")
	if conn.txStatus != 'T' {
		t.Fatalf("Expected txStatus to be 'T', instead it was '%c'", conn.txStatus)
	}

	pool.Release(conn)

	if conn.txStatus != 'I' {
		t.Fatalf("Expected release to rollback uncommitted transaction, but it did not: '%c'", conn.txStatus)
	}
}
