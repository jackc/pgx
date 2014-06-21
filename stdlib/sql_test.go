package stdlib_test

import (
	"database/sql"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"testing"
)

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", "postgres://pgx_md5:secret@localhost:5432/pgx_test")
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
		Host:     "localhost",
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
}

func TestConnQueryFailure(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Query("select 'foo")
	if _, ok := err.(pgx.PgError); !ok {
		t.Fatalf("Expected db.Query to return pgx.PgError, but instead received: %v", err)
	}
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
}
