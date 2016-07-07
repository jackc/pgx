package stdlib_test

import (
	"bytes"
	"database/sql"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"testing"
)

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", "postgres://pgx_md5:secret@127.0.0.1:5432/pgx_test")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	return db
}

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
		t.Fatalf("db.Query failed: ", err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("db.Query failed: ", err)
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

func TestSqlOpenDoesNotHavePool(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	driver := db.Driver().(*stdlib.Driver)
	if driver.Pool != nil {
		t.Fatal("Did not expect driver opened through database/sql to have Pool, but it did")
	}
}

func TestOpenFromConnPool(t *testing.T) {
	connConfig := pgx.ConnConfig{
		Host:     "127.0.0.1",
		User:     "pgx_md5",
		Password: "secret",
		Database: "pgx_test",
	}

	config := pgx.ConnPoolConfig{ConnConfig: connConfig}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	db, err := stdlib.OpenFromConnPool(pool)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	defer closeDB(t, db)

	// Can get pgx.ConnPool from driver
	driver := db.Driver().(*stdlib.Driver)
	if driver.Pool == nil {
		t.Fatal("Expected driver opened through OpenFromConnPool to have Pool, but it did not")
	}

	// Normal sql/database still works
	var n int64
	err = db.QueryRow("select 1").Scan(&n)
	if err != nil {
		t.Fatalf("db.QueryRow unexpectedly failed: %v", err)
	}
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

type testLog struct {
	lvl int
	msg string
	ctx []interface{}
}

type testLogger struct {
	logs []testLog
}

func (l *testLogger) Debug(msg string, ctx ...interface{}) {
	l.logs = append(l.logs, testLog{lvl: pgx.LogLevelDebug, msg: msg, ctx: ctx})
}
func (l *testLogger) Info(msg string, ctx ...interface{}) {
	l.logs = append(l.logs, testLog{lvl: pgx.LogLevelInfo, msg: msg, ctx: ctx})
}
func (l *testLogger) Warn(msg string, ctx ...interface{}) {
	l.logs = append(l.logs, testLog{lvl: pgx.LogLevelWarn, msg: msg, ctx: ctx})
}
func (l *testLogger) Error(msg string, ctx ...interface{}) {
	l.logs = append(l.logs, testLog{lvl: pgx.LogLevelError, msg: msg, ctx: ctx})
}

func TestConnQueryLog(t *testing.T) {
	logger := &testLogger{}

	connConfig := pgx.ConnConfig{
		Host:     "127.0.0.1",
		User:     "pgx_md5",
		Password: "secret",
		Database: "pgx_test",
		Logger:   logger,
	}

	config := pgx.ConnPoolConfig{ConnConfig: connConfig}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	db, err := stdlib.OpenFromConnPool(pool)
	if err != nil {
		t.Fatalf("Unable to create connection pool: %v", err)
	}
	defer closeDB(t, db)

	// clear logs from initial connection
	logger.logs = []testLog{}

	var n int64
	err = db.QueryRow("select 1").Scan(&n)
	if err != nil {
		t.Fatalf("db.QueryRow unexpectedly failed: %v", err)
	}

	l := logger.logs[0]
	if l.msg != "Query" {
		t.Errorf("Expected to log Query, but got %v", l)
	}

	if !(l.ctx[0] == "sql" && l.ctx[1] == "select 1") {
		t.Errorf("Expected to log Query with sql 'select 1', but got %v", l)
	}
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
	if _, ok := err.(pgx.PgError); !ok {
		t.Fatalf("Expected db.Query to return pgx.PgError, but instead received: %v", err)
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

	if !serverHasJSON(t, db) {
		t.Skip("Skipping due to server's lack of JSON type")
	}

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

	if !serverHasJSON(t, db) {
		t.Skip("Skipping due to server's lack of JSON type")
	}

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

func serverHasJSON(t *testing.T, db *sql.DB) bool {
	var hasJSON bool
	err := db.QueryRow(`select exists(select 1 from pg_type where typname='json')`).Scan(&hasJSON)
	if err != nil {
		t.Fatalf("db.QueryRow unexpectedly failed: %v", err)
	}
	return hasJSON
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
