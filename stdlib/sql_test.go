package stdlib_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeDB(t *testing.T, db *sql.DB) {
	err := db.Close()
	if err != nil {
		t.Fatalf("db.Close unexpectedly failed: %v", err)
	}
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, db *sql.DB) {
	var sum, rowCount int32

	rows, err := db.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("db.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("db.Query failed: %v", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

type preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

func prepareStmt(t *testing.T, p preparer, sql string) *sql.Stmt {
	stmt, err := p.Prepare(sql)
	if err != nil {
		t.Fatalf("%v Prepare unexpectedly failed: %v", p, err)
	}

	return stmt
}

func closeStmt(t *testing.T, stmt *sql.Stmt) {
	err := stmt.Close()
	if err != nil {
		t.Fatalf("stmt.Close unexpectedly failed: %v", err)
	}
}

func TestSQLOpen(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	closeDB(t, db)
}

func TestNormalLifeCycle(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt := prepareStmt(t, db, "select 'foo', n from generate_series($1::int, $2::int) n")
	defer closeStmt(t, stmt)

	rows, err := stmt.Query(int32(1), int32(10))
	if err != nil {
		t.Fatalf("stmt.Query unexpectedly failed: %v", err)
	}

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatalf("rows.Scan unexpectedly failed: %v", err)
		}
		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("rows.Err unexpectedly is: %v", err)
	}
	if rowCount != 10 {
		t.Fatalf("Expected to receive 10 rows, instead received %d", rowCount)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestStmtExec(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin unexpectedly failed: %v", err)
	}

	createStmt := prepareStmt(t, tx, "create temporary table t(a varchar not null)")
	_, err = createStmt.Exec()
	if err != nil {
		t.Fatalf("stmt.Exec unexpectedly failed: %v", err)
	}
	closeStmt(t, createStmt)

	insertStmt := prepareStmt(t, tx, "insert into t values($1::text)")
	result, err := insertStmt.Exec("foo")
	if err != nil {
		t.Fatalf("stmt.Exec unexpectedly failed: %v", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("result.RowsAffected unexpectedly failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Expected 1, received %d", n)
	}
	closeStmt(t, insertStmt)

	if err != nil {
		t.Fatalf("tx.Commit unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestQueryCloseRowsEarly(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt := prepareStmt(t, db, "select 'foo', n from generate_series($1::int, $2::int) n")
	defer closeStmt(t, stmt)

	rows, err := stmt.Query(int32(1), int32(10))
	if err != nil {
		t.Fatalf("stmt.Query unexpectedly failed: %v", err)
	}

	// Close rows immediately without having read them
	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	// Run the query again to ensure the connection and statement are still ok
	rows, err = stmt.Query(int32(1), int32(10))
	if err != nil {
		t.Fatalf("stmt.Query unexpectedly failed: %v", err)
	}

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatalf("rows.Scan unexpectedly failed: %v", err)
		}
		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("rows.Err unexpectedly is: %v", err)
	}
	if rowCount != 10 {
		t.Fatalf("Expected to receive 10 rows, instead received %d", rowCount)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestConnExec(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(a varchar not null)")
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	result, err := db.Exec("insert into t values('hey')")
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("result.RowsAffected unexpectedly failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Expected 1, received %d", n)
	}

	ensureConnValid(t, db)
}

func TestConnQuery(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	rows, err := db.Query("select 'foo', n from generate_series($1::int, $2::int) n", int32(1), int32(10))
	if err != nil {
		t.Fatalf("db.Query unexpectedly failed: %v", err)
	}

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatalf("rows.Scan unexpectedly failed: %v", err)
		}
		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("rows.Err unexpectedly is: %v", err)
	}
	if rowCount != 10 {
		t.Fatalf("Expected to receive 10 rows, instead received %d", rowCount)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestConnQueryNull(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	rows, err := db.Query("select $1::int", nil)
	if err != nil {
		t.Fatalf("db.Query unexpectedly failed: %v", err)
	}

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var n sql.NullInt64
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("rows.Scan unexpectedly failed: %v", err)
		}
		if n.Valid != false {
			t.Errorf("Expected n to be null, but it was %v", n)
		}
	}
	err = rows.Err()
	if err != nil {
		t.Fatalf("rows.Err unexpectedly is: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("Expected to receive 11 rows, instead received %d", rowCount)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestConnQueryRowByteSlice(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	expected := []byte{222, 173, 190, 239}
	var actual []byte

	err := db.QueryRow(`select E'\\xdeadbeef'::bytea`).Scan(&actual)
	if err != nil {
		t.Fatalf("db.QueryRow unexpectedly failed: %v", err)
	}

	if bytes.Compare(actual, expected) != 0 {
		t.Fatalf("Expected %v, but got %v", expected, actual)
	}

	ensureConnValid(t, db)
}

func TestConnQueryFailure(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Query("select 'foo")
	if _, ok := err.(*pgconn.PgError); !ok {
		t.Fatalf("Expected db.Query to return pgconn.PgError, but instead received: %v", err)
	}

	ensureConnValid(t, db)
}

// Test type that pgx would handle natively in binary, but since it is not a
// database/sql native type should be passed through as a string
func TestConnQueryRowPgxBinary(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	sql := "select $1::int4[]"
	expected := "{1,2,3}"
	var actual string

	err := db.QueryRow(sql, expected).Scan(&actual)
	if err != nil {
		t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
	}

	if actual != expected {
		t.Errorf(`Expected "%v", got "%v" (sql -> %v)`, expected, actual, sql)
	}

	ensureConnValid(t, db)
}

func TestConnQueryRowUnknownType(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	sql := "select $1::point"
	expected := "(1,2)"
	var actual string

	err := db.QueryRow(sql, expected).Scan(&actual)
	if err != nil {
		t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
	}

	if actual != expected {
		t.Errorf(`Expected "%v", got "%v" (sql -> %v)`, expected, actual, sql)
	}

	ensureConnValid(t, db)
}

func TestConnQueryJSONIntoByteSlice(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec(`
		create temporary table docs(
			body json not null
		);

		insert into docs(body) values('{"foo":"bar"}');
`)
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	sql := `select * from docs`
	expected := []byte(`{"foo":"bar"}`)
	var actual []byte

	err = db.QueryRow(sql).Scan(&actual)
	if err != nil {
		t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
	}

	if bytes.Compare(actual, expected) != 0 {
		t.Errorf(`Expected "%v", got "%v" (sql -> %v)`, string(expected), string(actual), sql)
	}

	_, err = db.Exec(`drop table docs`)
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestConnExecInsertByteSliceIntoJSON(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec(`
		create temporary table docs(
			body json not null
		);
`)
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	expected := []byte(`{"foo":"bar"}`)

	_, err = db.Exec(`insert into docs(body) values($1)`, expected)
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	var actual []byte
	err = db.QueryRow(`select body from docs`).Scan(&actual)
	if err != nil {
		t.Fatalf("db.QueryRow unexpectedly failed: %v", err)
	}

	if bytes.Compare(actual, expected) != 0 {
		t.Errorf(`Expected "%v", got "%v"`, string(expected), string(actual))
	}

	_, err = db.Exec(`drop table docs`)
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestTransactionLifeCycle(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(a varchar not null)")
	if err != nil {
		t.Fatalf("db.Exec unexpectedly failed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin unexpectedly failed: %v", err)
	}

	_, err = tx.Exec("insert into t values('hi')")
	if err != nil {
		t.Fatalf("tx.Exec unexpectedly failed: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("tx.Rollback unexpectedly failed: %v", err)
	}

	var n int64
	err = db.QueryRow("select count(*) from t").Scan(&n)
	if err != nil {
		t.Fatalf("db.QueryRow.Scan unexpectedly failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("Expected 0 rows due to rollback, instead found %d", n)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("db.Begin unexpectedly failed: %v", err)
	}

	_, err = tx.Exec("insert into t values('hi')")
	if err != nil {
		t.Fatalf("tx.Exec unexpectedly failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("tx.Commit unexpectedly failed: %v", err)
	}

	err = db.QueryRow("select count(*) from t").Scan(&n)
	if err != nil {
		t.Fatalf("db.QueryRow.Scan unexpectedly failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("Expected 1 rows due to rollback, instead found %d", n)
	}

	ensureConnValid(t, db)
}

func TestConnBeginTxIsolation(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	var defaultIsoLevel string
	err := db.QueryRow("show transaction_isolation").Scan(&defaultIsoLevel)
	if err != nil {
		t.Fatalf("QueryRow failed: %v", err)
	}

	supportedTests := []struct {
		sqlIso sql.IsolationLevel
		pgIso  string
	}{
		{sqlIso: sql.LevelDefault, pgIso: defaultIsoLevel},
		{sqlIso: sql.LevelReadUncommitted, pgIso: "read uncommitted"},
		{sqlIso: sql.LevelReadCommitted, pgIso: "read committed"},
		{sqlIso: sql.LevelRepeatableRead, pgIso: "repeatable read"},
		{sqlIso: sql.LevelSnapshot, pgIso: "repeatable read"},
		{sqlIso: sql.LevelSerializable, pgIso: "serializable"},
	}
	for i, tt := range supportedTests {
		func() {
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tt.sqlIso})
			if err != nil {
				t.Errorf("%d. BeginTx failed: %v", i, err)
				return
			}
			defer tx.Rollback()

			var pgIso string
			err = tx.QueryRow("show transaction_isolation").Scan(&pgIso)
			if err != nil {
				t.Errorf("%d. QueryRow failed: %v", i, err)
			}

			if pgIso != tt.pgIso {
				t.Errorf("%d. pgIso => %s, want %s", i, pgIso, tt.pgIso)
			}
		}()
	}

	unsupportedTests := []struct {
		sqlIso sql.IsolationLevel
	}{
		{sqlIso: sql.LevelWriteCommitted},
		{sqlIso: sql.LevelLinearizable},
	}
	for i, tt := range unsupportedTests {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tt.sqlIso})
		if err == nil {
			t.Errorf("%d. BeginTx should have failed", i)
			tx.Rollback()
		}
	}

	ensureConnValid(t, db)
}

func TestConnBeginTxReadOnly(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	var pgReadOnly string
	err = tx.QueryRow("show transaction_read_only").Scan(&pgReadOnly)
	if err != nil {
		t.Errorf("QueryRow failed: %v", err)
	}

	if pgReadOnly != "on" {
		t.Errorf("pgReadOnly => %s, want %s", pgReadOnly, "on")
	}

	ensureConnValid(t, db)
}

func TestBeginTxContextCancel(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("drop table if exists t")
	if err != nil {
		t.Fatalf("db.Exec failed: %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	_, err = tx.Exec("create table t(id serial)")
	if err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}

	cancelFn()

	err = tx.Commit()
	if err != context.Canceled && err != sql.ErrTxDone {
		t.Fatalf("err => %v, want %v or %v", err, context.Canceled, sql.ErrTxDone)
	}

	var n int
	err = db.QueryRow("select count(*) from t").Scan(&n)
	if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != "42P01" {
		t.Fatalf(`err => %v, want PgError{Code: "42P01"}`, err)
	}

	ensureConnValid(t, db)
}

func TestAcquireConn(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	var conns []*pgx.Conn

	for i := 1; i < 6; i++ {
		conn, err := stdlib.AcquireConn(db)
		if err != nil {
			t.Errorf("%d. AcquireConn failed: %v", i, err)
			continue
		}

		var n int32
		err = conn.QueryRow(context.Background(), "select 1").Scan(&n)
		if err != nil {
			t.Errorf("%d. QueryRow failed: %v", i, err)
		}
		if n != 1 {
			t.Errorf("%d. n => %d, want %d", i, n, 1)
		}

		stats := db.Stats()
		if stats.OpenConnections != i {
			t.Errorf("%d. stats.OpenConnections => %d, want %d", i, stats.OpenConnections, i)
		}

		conns = append(conns, conn)
	}

	for i, conn := range conns {
		if err := stdlib.ReleaseConn(db, conn); err != nil {
			t.Errorf("%d. stdlib.ReleaseConn failed: %v", i, err)
		}
	}

	ensureConnValid(t, db)
}

// https://github.com/jackc/pgx/issues/673
func TestReleaseConnWithTxInProgress(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	c1, err := stdlib.AcquireConn(db)
	require.NoError(t, err)

	_, err = c1.Exec(context.Background(), "begin")
	require.NoError(t, err)

	c1PID := c1.PgConn().PID()

	err = stdlib.ReleaseConn(db, c1)
	require.NoError(t, err)

	c2, err := stdlib.AcquireConn(db)
	require.NoError(t, err)

	c2PID := c2.PgConn().PID()

	err = stdlib.ReleaseConn(db, c2)
	require.NoError(t, err)

	require.NotEqual(t, c1PID, c2PID)

	// Releasing a conn with a tx in progress should close the connection
	stats := db.Stats()
	require.Equal(t, 1, stats.OpenConnections)

	ensureConnValid(t, db)
}

func TestConnPingContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("db.PingContext failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestConnPrepareContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt, err := db.PrepareContext(context.Background(), "select now()")
	if err != nil {
		t.Fatalf("db.PrepareContext failed: %v", err)
	}
	stmt.Close()

	ensureConnValid(t, db)
}

func TestConnExecContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.ExecContext(context.Background(), "create temporary table exec_context_test(id serial primary key)")
	if err != nil {
		t.Fatalf("db.ExecContext failed: %v", err)
	}

	ensureConnValid(t, db)
}

func TestConnExecContextFailureRetry(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	// we get a connection, immediately close it, and then get it back
	{
		conn, err := stdlib.AcquireConn(db)
		if err != nil {
			t.Fatalf("stdlib.AcquireConn unexpectedly failed: %v", err)
		}
		conn.Close(context.Background())
		stdlib.ReleaseConn(db, conn)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn unexpectedly failed: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "select 1"); err != driver.ErrBadConn {
		t.Fatalf("Expected conn.ExecContext to return driver.ErrBadConn, but instead received: %v", err)
	}
}

func TestConnQueryContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	rows, err := db.QueryContext(context.Background(), "select * from generate_series(1,10) n")
	if err != nil {
		t.Fatalf("db.QueryContext failed: %v", err)
	}

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
	}

	if rows.Err() != nil {
		t.Error(rows.Err())
	}

	ensureConnValid(t, db)
}

func TestConnQueryContextFailureRetry(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	// we get a connection, immediately close it, and then get it back
	{
		conn, err := stdlib.AcquireConn(db)
		if err != nil {
			t.Fatalf("stdlib.AcquireConn unexpectedly failed: %v", err)
		}
		conn.Close(context.Background())
		stdlib.ReleaseConn(db, conn)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("db.Conn unexpectedly failed: %v", err)
	}
	if _, err := conn.QueryContext(context.Background(), "select 1"); err != driver.ErrBadConn {
		t.Fatalf("Expected conn.QueryContext to return driver.ErrBadConn, but instead received: %v", err)
	}
}

func TestRowsColumnTypeDatabaseTypeName(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	rows, err := db.Query("select * from generate_series(1,10) n")
	if err != nil {
		t.Fatalf("db.Query failed: %v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("rows.ColumnTypes failed: %v", err)
	}

	if len(columnTypes) != 1 {
		t.Fatalf("len(columnTypes) => %v, want %v", len(columnTypes), 1)
	}

	if columnTypes[0].DatabaseTypeName() != "INT4" {
		t.Errorf("columnTypes[0].DatabaseTypeName() => %v, want %v", columnTypes[0].DatabaseTypeName(), "INT4")
	}

	rows.Close()

	ensureConnValid(t, db)
}

func TestStmtExecContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(id int primary key)")
	if err != nil {
		t.Fatalf("db.Exec failed: %v", err)
	}

	stmt, err := db.Prepare("insert into t(id) values ($1::int4)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}

	ensureConnValid(t, db)
}

func TestStmtExecContextCancel(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(id int primary key)")
	if err != nil {
		t.Fatalf("db.Exec failed: %v", err)
	}

	stmt, err := db.Prepare("insert into t(id) select $1::int4 from pg_sleep(5)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = stmt.ExecContext(ctx, 42)
	if !pgconn.Timeout(err) {
		t.Errorf("expected timeout error, got %v", err)
	}

	ensureConnValid(t, db)
}

func TestStmtQueryContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt, err := db.Prepare("select * from generate_series(1,$1::int4) n")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background(), 5)
	if err != nil {
		t.Fatalf("stmt.QueryContext failed: %v", err)
	}

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
	}

	if rows.Err() != nil {
		t.Error(rows.Err())
	}

	ensureConnValid(t, db)
}

func TestRowsColumnTypes(t *testing.T) {
	columnTypesTests := []struct {
		Name     string
		TypeName string
		Length   struct {
			Len int64
			OK  bool
		}
		DecimalSize struct {
			Precision int64
			Scale     int64
			OK        bool
		}
		ScanType reflect.Type
	}{
		{
			Name:     "a",
			TypeName: "INT4",
			Length: struct {
				Len int64
				OK  bool
			}{
				Len: 0,
				OK:  false,
			},
			DecimalSize: struct {
				Precision int64
				Scale     int64
				OK        bool
			}{
				Precision: 0,
				Scale:     0,
				OK:        false,
			},
			ScanType: reflect.TypeOf(int32(0)),
		}, {
			Name:     "bar",
			TypeName: "TEXT",
			Length: struct {
				Len int64
				OK  bool
			}{
				Len: math.MaxInt64,
				OK:  true,
			},
			DecimalSize: struct {
				Precision int64
				Scale     int64
				OK        bool
			}{
				Precision: 0,
				Scale:     0,
				OK:        false,
			},
			ScanType: reflect.TypeOf(""),
		}, {
			Name:     "dec",
			TypeName: "NUMERIC",
			Length: struct {
				Len int64
				OK  bool
			}{
				Len: 0,
				OK:  false,
			},
			DecimalSize: struct {
				Precision int64
				Scale     int64
				OK        bool
			}{
				Precision: 9,
				Scale:     2,
				OK:        true,
			},
			ScanType: reflect.TypeOf(float64(0)),
		},
	}

	db := openDB(t)
	defer closeDB(t, db)

	rows, err := db.Query("SELECT 1 AS a, text 'bar' AS bar, 1.28::numeric(9, 2) AS dec")
	if err != nil {
		t.Fatal(err)
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 3 {
		t.Errorf("expected 3 columns found %d", len(columns))
	}

	for i, tt := range columnTypesTests {
		c := columns[i]
		if c.Name() != tt.Name {
			t.Errorf("(%d) got: %s, want: %s", i, c.Name(), tt.Name)
		}
		if c.DatabaseTypeName() != tt.TypeName {
			t.Errorf("(%d) got: %s, want: %s", i, c.DatabaseTypeName(), tt.TypeName)
		}
		l, ok := c.Length()
		if l != tt.Length.Len {
			t.Errorf("(%d) got: %d, want: %d", i, l, tt.Length.Len)
		}
		if ok != tt.Length.OK {
			t.Errorf("(%d) got: %t, want: %t", i, ok, tt.Length.OK)
		}
		p, s, ok := c.DecimalSize()
		if p != tt.DecimalSize.Precision {
			t.Errorf("(%d) got: %d, want: %d", i, p, tt.DecimalSize.Precision)
		}
		if s != tt.DecimalSize.Scale {
			t.Errorf("(%d) got: %d, want: %d", i, s, tt.DecimalSize.Scale)
		}
		if ok != tt.DecimalSize.OK {
			t.Errorf("(%d) got: %t, want: %t", i, ok, tt.DecimalSize.OK)
		}
		if c.ScanType() != tt.ScanType {
			t.Errorf("(%d) got: %v, want: %v", i, c.ScanType(), tt.ScanType)
		}
	}
}

func TestSimpleQueryLifeCycle(t *testing.T) {
	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatalf("pgx.ParseConnectionString failed: %v", err)
	}
	config.PreferSimpleProtocol = true

	db := stdlib.OpenDB(*config)
	defer closeDB(t, db)

	rows, err := db.Query("SELECT 'foo', n FROM generate_series($1::int, $2::int) n WHERE 3 = $3", 1, 10, 3)
	if err != nil {
		t.Fatalf("stmt.Query unexpectedly failed: %v", err)
	}

	rowCount := int64(0)

	for rows.Next() {
		rowCount++
		var (
			s string
			n int64
		)

		if err := rows.Scan(&s, &n); err != nil {
			t.Fatalf("rows.Scan unexpectedly failed: %v", err)
		}

		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}

		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("rows.Err unexpectedly is: %v", err)
	}

	if rowCount != 10 {
		t.Fatalf("Expected to receive 10 rows, instead received %d", rowCount)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	rows, err = db.Query("select 1 where false")
	if err != nil {
		t.Fatalf("stmt.Query unexpectedly failed: %v", err)
	}

	rowCount = int64(0)

	for rows.Next() {
		rowCount++
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("rows.Err unexpectedly is: %v", err)
	}

	if rowCount != 0 {
		t.Fatalf("Expected to receive 10 rows, instead received %d", rowCount)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("rows.Close unexpectedly failed: %v", err)
	}

	ensureConnValid(t, db)
}

// https://github.com/jackc/pgx/issues/409
func TestScanJSONIntoJSONRawMessage(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	var msg json.RawMessage

	err := db.QueryRow("select '{}'::json").Scan(&msg)
	if err != nil {
		t.Fatalf("QueryRow / Scan failed: %v", err)
	}

	if bytes.Compare([]byte("{}"), []byte(msg)) != 0 {
		t.Fatalf("Expected %v, got %v", []byte("{}"), msg)
	}

	ensureConnValid(t, db)
}

type testLog struct {
	lvl  pgx.LogLevel
	msg  string
	data map[string]interface{}
}

type testLogger struct {
	logs []testLog
}

func (l *testLogger) Log(ctx context.Context, lvl pgx.LogLevel, msg string, data map[string]interface{}) {
	l.logs = append(l.logs, testLog{lvl: lvl, msg: msg, data: data})
}

func TestRegisterConnConfig(t *testing.T) {
	connConfig, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	logger := &testLogger{}
	connConfig.Logger = logger

	connStr := stdlib.RegisterConnConfig(connConfig)
	defer stdlib.UnregisterConnConfig(connStr)

	db, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	defer closeDB(t, db)

	var n int64
	err = db.QueryRow("select 1").Scan(&n)
	require.NoError(t, err)

	l := logger.logs[len(logger.logs)-1]
	assert.Equal(t, "Query", l.msg)
	assert.Equal(t, "select 1", l.data["sql"])
}
