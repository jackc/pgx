package pgx_test

import (
	"github.com/jackc/pgx"
	"testing"
)

func TestTransactionSuccessfulCommit(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec("insert into foo(id) values (1)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	var n int64
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestTransactionSuccessfulRollback(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec("insert into foo(id) values (1)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("tx.Rollback failed: %v", err)
	}

	var n int64
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestBeginIso(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	isoLevels := []string{pgx.Serializable, pgx.RepeatableRead, pgx.ReadCommitted, pgx.ReadUncommitted}
	for _, iso := range isoLevels {
		tx, err := conn.BeginIso(iso)
		if err != nil {
			t.Fatalf("conn.BeginIso failed: %v", err)
		}

		var level string
		conn.QueryRow("select current_setting('transaction_isolation')").Scan(&level)
		if level != iso {
			t.Errorf("Expected to be in isolation level %v but was %v", iso, level)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatalf("tx.Rollback failed: %v", err)
		}
	}
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Transaction happy path -- it executes function and commits
	committed, err := conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		return true
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: %v", err)
	}
	if !committed {
		t.Fatal("Transaction was not committed")
	}

	var n int64
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	mustExec(t, conn, "truncate foo")

	// It rolls back when passed function returns false
	committed, err = conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		return false
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: %v", err)
	}
	if committed {
		t.Fatal("Transaction should not have been committed")
	}
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	// it rolls back changes when connection is in error state
	committed, err = conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		if _, err := conn.Exec("invalid"); err == nil {
			t.Fatal("Exec was supposed to error but didn't")
		}
		return true
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: %v", err)
	}
	if committed {
		t.Fatal("Transaction was committed when it shouldn't have been")
	}
	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	// when commit fails
	committed, err = conn.Transaction(func() bool {
		mustExec(t, conn, "insert into foo(id) values (1)")
		mustExec(t, conn, "insert into foo(id) values (1)")
		return true
	})
	if err == nil {
		t.Fatal("Transaction should have failed but didn't")
	}
	if committed {
		t.Fatal("Transaction was committed when it should have failed")
	}

	err = conn.QueryRow("select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	// when something in transaction panics
	func() {
		defer func() {
			recover()
		}()

		committed, err = conn.Transaction(func() bool {
			mustExec(t, conn, "insert into foo(id) values (1)")
			panic("stop!")
		})

		err = conn.QueryRow("select count(*) from foo").Scan(&n)
		if err != nil {
			t.Fatalf("QueryRow Scan failed: %v", err)
		}
		if n != 0 {
			t.Fatalf("Did not receive correct number of rows: %v", n)
		}
	}()
}

func TestTransactionIso(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	isoLevels := []string{pgx.Serializable, pgx.RepeatableRead, pgx.ReadCommitted, pgx.ReadUncommitted}
	for _, iso := range isoLevels {
		_, err := conn.TransactionIso(iso, func() bool {
			var level string
			conn.QueryRow("select current_setting('transaction_isolation')").Scan(&level)
			if level != iso {
				t.Errorf("Expected to be in isolation level %v but was %v", iso, level)
			}
			return true
		})
		if err != nil {
			t.Fatalf("Unexpected transaction failure: %v", err)
		}
	}
}
