package pgx_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func TestTransactionSuccessfulCommit(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id)
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin(context.Background())
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
      unique (id)
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin(context.Background())
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

func TestTxCommitWhenDeferredConstraintFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip("Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")
	}

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}

	if _, err := tx.Exec(context.Background(), "insert into foo(id) values (1)"); err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	if _, err := tx.Exec(context.Background(), "insert into foo(id) values (1)"); err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	err = tx.Commit(context.Background())
	if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != "23505" {
		t.Fatalf("Expected unique constraint violation 23505, got %#v", err)
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

	tx1, err := c1.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx1.Rollback(context.Background())

	tx2, err := c2.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
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

	ensureConnValid(t, c1)
	ensureConnValid(t, c2)
}

func TestTransactionSuccessfulRollback(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id)
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin(context.Background())
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

func TestTransactionRollbackFailsClosesConnection(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancel := context.WithCancel(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	cancel()

	err = tx.Rollback(ctx)
	require.Error(t, err)

	require.True(t, conn.IsClosed())
}

func TestBeginIsoLevels(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip("Server always uses SERIALIZABLE isolation (https://www.cockroachlabs.com/docs/stable/demo-serializable.html)")
	}

	isoLevels := []pgx.TxIsoLevel{pgx.Serializable, pgx.RepeatableRead, pgx.ReadCommitted, pgx.ReadUncommitted}
	for _, iso := range isoLevels {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: iso})
		if err != nil {
			t.Fatalf("conn.Begin failed: %v", err)
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

func TestBeginFunc(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	_, err := conn.Exec(context.Background(), createSql)
	require.NoError(t, err)

	err = conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(), "insert into foo(id) values (1)")
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	var n int64
	err = conn.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
}

func TestBeginFuncRollbackOnError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	_, err := conn.Exec(context.Background(), createSql)
	require.NoError(t, err)

	err = conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(), "insert into foo(id) values (1)")
		require.NoError(t, err)
		return errors.New("some error")
	})
	require.EqualError(t, err, "some error")

	var n int64
	err = conn.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 0, n)
}

func TestBeginReadOnly(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("conn.Begin failed: %v", err)
	}
	defer tx.Rollback(context.Background())

	_, err = conn.Exec(context.Background(), "create table foo(id serial primary key)")
	if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != "25006" {
		t.Errorf("Expected error SQLSTATE 25006, but got %#v", err)
	}
}

func TestTxNestedTransactionCommit(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id)
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(context.Background(), "insert into foo(id) values (1)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	nestedTx, err := tx.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = nestedTx.Exec(context.Background(), "insert into foo(id) values (2)")
	if err != nil {
		t.Fatalf("nestedTx.Exec failed: %v", err)
	}

	doubleNestedTx, err := nestedTx.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = doubleNestedTx.Exec(context.Background(), "insert into foo(id) values (3)")
	if err != nil {
		t.Fatalf("doubleNestedTx.Exec failed: %v", err)
	}

	err = doubleNestedTx.Commit(context.Background())
	if err != nil {
		t.Fatalf("doubleNestedTx.Commit failed: %v", err)
	}

	err = nestedTx.Commit(context.Background())
	if err != nil {
		t.Fatalf("nestedTx.Commit failed: %v", err)
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
	if n != 3 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestTxNestedTransactionRollback(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id)
    );
  `

	if _, err := conn.Exec(context.Background(), createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(context.Background(), "insert into foo(id) values (1)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	nestedTx, err := tx.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, err = nestedTx.Exec(context.Background(), "insert into foo(id) values (2)")
	if err != nil {
		t.Fatalf("nestedTx.Exec failed: %v", err)
	}

	err = nestedTx.Rollback(context.Background())
	if err != nil {
		t.Fatalf("nestedTx.Rollback failed: %v", err)
	}

	_, err = tx.Exec(context.Background(), "insert into foo(id) values (3)")
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
	if n != 2 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}
}

func TestTxBeginFuncNestedTransactionCommit(t *testing.T) {
	t.Parallel()

	db := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, db)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	_, err := db.Exec(context.Background(), createSql)
	require.NoError(t, err)

	err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
		_, err := db.Exec(context.Background(), "insert into foo(id) values (1)")
		require.NoError(t, err)

		err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
			_, err := db.Exec(context.Background(), "insert into foo(id) values (2)")
			require.NoError(t, err)

			err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
				_, err := db.Exec(context.Background(), "insert into foo(id) values (3)")
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
	err = db.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
}

func TestTxBeginFuncNestedTransactionRollback(t *testing.T) {
	t.Parallel()

	db := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, db)

	createSql := `
    create temporary table foo(
      id integer,
      unique (id) initially deferred
    );
  `

	_, err := db.Exec(context.Background(), createSql)
	require.NoError(t, err)

	err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
		_, err := db.Exec(context.Background(), "insert into foo(id) values (1)")
		require.NoError(t, err)

		err = db.BeginFunc(context.Background(), func(db pgx.Tx) error {
			_, err := db.Exec(context.Background(), "insert into foo(id) values (2)")
			require.NoError(t, err)
			return errors.New("do a rollback")
		})
		require.EqualError(t, err, "do a rollback")

		_, err = db.Exec(context.Background(), "insert into foo(id) values (3)")
		require.NoError(t, err)

		return nil
	})

	var n int64
	err = db.QueryRow(context.Background(), "select count(*) from foo").Scan(&n)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
}
