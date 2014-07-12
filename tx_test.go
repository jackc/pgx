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
