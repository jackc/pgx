package pgx_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

func TestTransactionSuccessfulCommit(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec(context.Background(), "insert into foo(id) values (1)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	var n int64
	err = conn.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestTxCommitWhenTxBroken(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}

	if _, err := tx.Exec(context.Background(), "insert into foo(id) values (1)"); err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	// Purposely break transaction
	if _, err := tx.Exec(context.Background(), "syntax error"); err == nil {
		t.Fatal("Unexpected success")
	}

	err = tx.Commit(context.Background())
	if err != pgx.ErrTxCommitRollback {
		t.Fatalf("Expected error %v, got %v", pgx.ErrTxCommitRollback, err)
	}

	var n int64
	err = conn.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestTxCommitSerializationFailure(t *testing.T) {
	t.Parallel()

	c1 := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, c1)

	c2 := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, c2)

	c1.Exec(context.Background(), `drop table if exists tx_serializable_sums`)
	_, err := c1.Exec(context.Background(), `create table tx_serializable_sums(num integer);`)
	if err != nil {
		t.Fatalf("Unable to create temporary table: %v", err)
	}
	defer c1.Exec(context.Background(), `drop table tx_serializable_sums`)

	tx1, err := c1.BeginEx(context.Background(), &pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("BeginEx failed: %v", err)
	}
	defer tx1.Rollback(context.Background())

	tx2, err := c2.BeginEx(context.Background(), &pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("BeginEx failed: %v", err)
	}
	defer tx2.Rollback(context.Background())

	_, err = tx1.Exec(context.Background(), `insert into tx_serializable_sums(num) select sum(num) from tx_serializable_sums`)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	_, err = tx2.Exec(context.Background(), `insert into tx_serializable_sums(num) select sum(num) from tx_serializable_sums`)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	err = tx1.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	err = tx2.Commit(context.Background())
	if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != "40001" {
		t.Fatalf("Expected serialization error 40001, got %#v", err)
	}
}

func TestTransactionSuccessfulRollback(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec(context.Background(), "insert into foo(id) values (1)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	err = tx.Rollback(context.Background())
	if err != nil {
		t.Fatalf("tx.Rollback failed: %v", err)
	}

	var n int64
	err = conn.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestBeginExIsoLevels(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	isoLevels := []pgx.TxIsoLevel{pgx.Serializable, pgx.RepeatableRead, pgx.ReadCommitted, pgx.ReadUncommitted}
	for _, iso := range isoLevels {
		tx, err := conn.BeginEx(context.Background(), &pgx.TxOptions{IsoLevel: iso})
		if err != nil {
			t.Fatalf("conn.BeginEx failed: %v", err)
		}

		var level pgx.TxIsoLevel
		conn.QueryRow(context.Background(), "select current_setting('transaction_isolation')").Scan(&level)
		if level != iso {
			t.Errorf("Expected to be in isolation level %v but was %v", iso, level)
		}

		err = tx.Rollback(context.Background())
		if err != nil {
			t.Fatalf("tx.Rollback failed: %v", err)
		}
	}
}

func TestBeginExReadOnly(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.BeginEx(context.Background(), &pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("conn.BeginEx failed: %v", err)
	}
	defer tx.Rollback(context.Background())

	_, err = conn.Exec(context.Background(), "create table foo(id serial primary key)")
	if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != "25006" {
		t.Errorf("Expected error SQLSTATE 25006, but got %#v", err)
	}
}

func TestTxStatus(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusInProgress {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusInProgress, status)
	}

	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusRollbackSuccess {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusRollbackSuccess, status)
	}
}

func TestTxStatusErrorInTransactions(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusInProgress {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusInProgress, status)
	}

	_, err = tx.Exec(context.Background(), "savepoint s")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(context.Background(), "syntax error")
	if err == nil {
		t.Fatal("expected an error but did not get one")
	}

	if status := tx.Status(); status != pgx.TxStatusInFailure {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusInFailure, status)
	}

	_, err = tx.Exec(context.Background(), "rollback to s")
	if err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusInProgress {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusInProgress, status)
	}

	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	if status := tx.Status(); status != pgx.TxStatusRollbackSuccess {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusRollbackSuccess, status)
	}
}

func TestTxErr(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Purposely break transaction
	if _, err := tx.Exec(context.Background(), "syntax error"); err == nil {
		t.Fatal("Unexpected success")
	}

	if err := tx.Commit(context.Background()); err != pgx.ErrTxCommitRollback {
		t.Fatalf("Expected error %v, got %v", pgx.ErrTxCommitRollback, err)
	}

	if status := tx.Status(); status != pgx.TxStatusCommitFailure {
		t.Fatalf("Expected status to be %v, but it was %v", pgx.TxStatusRollbackSuccess, status)
	}

	if err := tx.Err(); err != pgx.ErrTxCommitRollback {
		t.Fatalf("Expected error %v, got %v", pgx.ErrTxCommitRollback, err)
	}
}
