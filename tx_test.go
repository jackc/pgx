package pgx_test

import (
	"github.com/jackc/pgx"
	"testing"
	"time"
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

func TestTxCommitWhenTxBroken(t *testing.T) {
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

	if _, err := tx.Exec("insert into foo(id) values (1)"); err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	// Purposely break transaction
	if _, err := tx.Exec("syntax error"); err == nil {
		t.Fatal("Unexpected success")
	}

	err = tx.Commit()
	if err != pgx.ErrTxCommitRollback {
		t.Fatalf("Expected error %v, got %v", pgx.ErrTxCommitRollback, err)
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

func TestTxCommitSerializationFailure(t *testing.T) {
	t.Parallel()

	pool := createConnPool(t, 5)
	defer pool.Close()

	pool.Exec(`drop table if exists tx_serializable_sums`)
	_, err := pool.Exec(`create table tx_serializable_sums(num integer);`)
	if err != nil {
		t.Fatalf("Unable to create temporary table: %v", err)
	}
	defer pool.Exec(`drop table tx_serializable_sums`)

	tx1, err := pool.BeginIso(pgx.Serializable)
	if err != nil {
		t.Fatalf("BeginIso failed: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := pool.BeginIso(pgx.Serializable)
	if err != nil {
		t.Fatalf("BeginIso failed: %v", err)
	}
	defer tx2.Rollback()

	_, err = tx1.Exec(`insert into tx_serializable_sums(num) select sum(num) from tx_serializable_sums`)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	_, err = tx2.Exec(`insert into tx_serializable_sums(num) select sum(num) from tx_serializable_sums`)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	err = tx2.Commit()
	if pgErr, ok := err.(pgx.PgError); !ok || pgErr.Code != "40001" {
		t.Fatalf("Expected serialization error 40001, got %#v", err)
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

func TestTxAfterClose(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	var zeroTime, t1, t2 time.Time
	tx.AfterClose(func(tx *pgx.Tx) {
		t1 = time.Now()
	})

	tx.AfterClose(func(tx *pgx.Tx) {
		t2 = time.Now()
	})

	tx.Rollback()

	if t1 == zeroTime {
		t.Error("First Tx.AfterClose callback not called")
	}

	if t2 == zeroTime {
		t.Error("Second Tx.AfterClose callback not called")
	}

	if t1.Before(t2) {
		t.Errorf("AfterClose callbacks called out of order: %v, %v", t1, t2)
	}
}

func TestTxStatus(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusInProgress {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusInProgress, status)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusRollbackSuccess {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusRollbackSuccess, status)
	}
}

func TestTxErr(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Purposely break transaction
	if _, err := tx.Exec("syntax error"); err == nil {
		t.Fatal("Unexpected success")
	}

	if err := tx.Commit(); err != pgx.ErrTxCommitRollback {
		t.Fatalf("Expected error %v, got %v", pgx.ErrTxCommitRollback, err)
	}

	if status := tx.Status(); status != pgx.TxStatusCommitFailure {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusRollbackSuccess, status)
	}

	if err := tx.Err(); err != pgx.ErrTxCommitRollback {
		t.Fatalf("Expected error %v, got %v", pgx.ErrTxCommitRollback, err)
	}
}
