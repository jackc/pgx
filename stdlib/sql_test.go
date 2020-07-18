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

func openDB(t testing.TB) *sql.DB {
	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	return stdlib.OpenDB(*config)
}

func closeDB(t testing.TB, db *sql.DB) {
	err := db.Close()
	require.NoError(t, err)
}

func testWithAndWithoutPreferSimpleProtocol(t *testing.T, f func(t *testing.T, db *sql.DB)) {
	t.Run("SimpleProto",
		func(t *testing.T) {
			config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
			require.NoError(t, err)

			config.PreferSimpleProtocol = true
			db := stdlib.OpenDB(*config)
			defer func() {
				err := db.Close()
				require.NoError(t, err)
			}()

			f(t, db)

			ensureDBValid(t, db)
		},
	)

	t.Run("DefaultProto",
		func(t *testing.T) {
			config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
			require.NoError(t, err)

			db := stdlib.OpenDB(*config)
			defer func() {
				err := db.Close()
				require.NoError(t, err)
			}()

			f(t, db)

			ensureDBValid(t, db)
		},
	)
}

// Do a simple query to ensure the DB is still usable. This is of less use in stdlib as the connection pool should
// cover an broken connections.
func ensureDBValid(t testing.TB, db *sql.DB) {
	var sum, rowCount int32

	rows, err := db.Query("select generate_series(1,$1)", 10)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	require.NoError(t, rows.Err())

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
	require.NoError(t, err)
	return stmt
}

func closeStmt(t *testing.T, stmt *sql.Stmt) {
	err := stmt.Close()
	require.NoError(t, err)
}

func TestSQLOpen(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	closeDB(t, db)
}

func TestNormalLifeCycle(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt := prepareStmt(t, db, "select 'foo', n from generate_series($1::int, $2::int) n")
	defer closeStmt(t, stmt)

	rows, err := stmt.Query(int32(1), int32(10))
	require.NoError(t, err)

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		err := rows.Scan(&s, &n)
		require.NoError(t, err)

		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	require.NoError(t, rows.Err())

	require.EqualValues(t, 10, rowCount)

	err = rows.Close()
	require.NoError(t, err)

	ensureDBValid(t, db)
}

func TestStmtExec(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	tx, err := db.Begin()
	require.NoError(t, err)

	createStmt := prepareStmt(t, tx, "create temporary table t(a varchar not null)")
	_, err = createStmt.Exec()
	require.NoError(t, err)
	closeStmt(t, createStmt)

	insertStmt := prepareStmt(t, tx, "insert into t values($1::text)")
	result, err := insertStmt.Exec("foo")
	require.NoError(t, err)

	n, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	closeStmt(t, insertStmt)

	ensureDBValid(t, db)
}

func TestQueryCloseRowsEarly(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt := prepareStmt(t, db, "select 'foo', n from generate_series($1::int, $2::int) n")
	defer closeStmt(t, stmt)

	rows, err := stmt.Query(int32(1), int32(10))
	require.NoError(t, err)

	// Close rows immediately without having read them
	err = rows.Close()
	require.NoError(t, err)

	// Run the query again to ensure the connection and statement are still ok
	rows, err = stmt.Query(int32(1), int32(10))
	require.NoError(t, err)

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		err := rows.Scan(&s, &n)
		require.NoError(t, err)
		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	require.NoError(t, rows.Err())
	require.EqualValues(t, 10, rowCount)

	err = rows.Close()
	require.NoError(t, err)

	ensureDBValid(t, db)
}

func TestConnExec(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("create temporary table t(a varchar not null)")
		require.NoError(t, err)

		result, err := db.Exec("insert into t values('hey')")
		require.NoError(t, err)

		n, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	})
}

func TestConnQuery(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("select 'foo', n from generate_series($1::int, $2::int) n", int32(1), int32(10))
		require.NoError(t, err)

		rowCount := int64(0)

		for rows.Next() {
			rowCount++

			var s string
			var n int64
			err := rows.Scan(&s, &n)
			require.NoError(t, err)
			if s != "foo" {
				t.Errorf(`Expected "foo", received "%v"`, s)
			}
			if n != rowCount {
				t.Errorf("Expected %d, received %d", rowCount, n)
			}
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 10, rowCount)

		err = rows.Close()
		require.NoError(t, err)
	})
}

// https://github.com/jackc/pgx/issues/781
func TestConnQueryDifferentScanPlansIssue781(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		var s string
		var b bool

		rows, err := db.Query("select true, 'foo'")
		require.NoError(t, err)

		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&b, &s))
		assert.Equal(t, true, b)
		assert.Equal(t, "foo", s)
	})
}

func TestConnQueryNull(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("select $1::int", nil)
		require.NoError(t, err)

		rowCount := int64(0)

		for rows.Next() {
			rowCount++

			var n sql.NullInt64
			err := rows.Scan(&n)
			require.NoError(t, err)
			if n.Valid != false {
				t.Errorf("Expected n to be null, but it was %v", n)
			}
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 1, rowCount)

		err = rows.Close()
		require.NoError(t, err)
	})
}

func TestConnQueryRowByteSlice(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		expected := []byte{222, 173, 190, 239}
		var actual []byte

		err := db.QueryRow(`select E'\\xdeadbeef'::bytea`).Scan(&actual)
		require.NoError(t, err)
		require.EqualValues(t, expected, actual)
	})
}

func TestConnQueryFailure(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Query("select 'foo")
		require.Error(t, err)
		require.IsType(t, new(pgconn.PgError), err)
	})
}

func TestConnSimpleSlicePassThrough(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		var n int64
		err := db.QueryRow("select cardinality($1::text[])", []string{"a", "b", "c"}).Scan(&n)
		require.NoError(t, err)
		assert.EqualValues(t, 3, n)
	})
}

// Test type that pgx would handle natively in binary, but since it is not a
// database/sql native type should be passed through as a string
func TestConnQueryRowPgxBinary(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		sql := "select $1::int4[]"
		expected := "{1,2,3}"
		var actual string

		err := db.QueryRow(sql, expected).Scan(&actual)
		require.NoError(t, err)
		require.EqualValues(t, expected, actual)
	})
}

func TestConnQueryRowUnknownType(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		sql := "select $1::point"
		expected := "(1,2)"
		var actual string

		err := db.QueryRow(sql, expected).Scan(&actual)
		require.NoError(t, err)
		require.EqualValues(t, expected, actual)
	})
}

func TestConnQueryJSONIntoByteSlice(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec(`
		create temporary table docs(
			body json not null
		);

		insert into docs(body) values('{"foo":"bar"}');
`)
		require.NoError(t, err)

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
		require.NoError(t, err)
	})
}

func TestConnExecInsertByteSliceIntoJSON(t *testing.T) {
	// Not testing with simple protocol because there is no way for that to work. A []byte will be considered binary data
	// that needs to escape. No way to know whether the destination is really a text compatible or a bytea.

	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec(`
		create temporary table docs(
			body json not null
		);
`)
	require.NoError(t, err)

	expected := []byte(`{"foo":"bar"}`)

	_, err = db.Exec(`insert into docs(body) values($1)`, expected)
	require.NoError(t, err)

	var actual []byte
	err = db.QueryRow(`select body from docs`).Scan(&actual)
	require.NoError(t, err)

	if bytes.Compare(actual, expected) != 0 {
		t.Errorf(`Expected "%v", got "%v"`, string(expected), string(actual))
	}

	_, err = db.Exec(`drop table docs`)
	require.NoError(t, err)
}

func TestTransactionLifeCycle(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("create temporary table t(a varchar not null)")
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("insert into t values('hi')")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var n int64
		err = db.QueryRow("select count(*) from t").Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 0, n)

		tx, err = db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("insert into t values('hi')")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.QueryRow("select count(*) from t").Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	})
}

func TestConnBeginTxIsolation(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		var defaultIsoLevel string
		err := db.QueryRow("show transaction_isolation").Scan(&defaultIsoLevel)
		require.NoError(t, err)

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
	})
}

func TestConnBeginTxReadOnly(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		require.NoError(t, err)
		defer tx.Rollback()

		var pgReadOnly string
		err = tx.QueryRow("show transaction_read_only").Scan(&pgReadOnly)
		if err != nil {
			t.Errorf("QueryRow failed: %v", err)
		}

		if pgReadOnly != "on" {
			t.Errorf("pgReadOnly => %s, want %s", pgReadOnly, "on")
		}
	})
}

func TestBeginTxContextCancel(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("drop table if exists t")
		require.NoError(t, err)

		ctx, cancelFn := context.WithCancel(context.Background())

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.Exec("create table t(id serial)")
		require.NoError(t, err)

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
	})
}

func TestAcquireConn(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
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
	})
}

func TestConnRaw(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		var n int
		err = conn.Raw(func(driverConn interface{}) error {
			conn := driverConn.(*stdlib.Conn).Conn()
			return conn.QueryRow(context.Background(), "select 42").Scan(&n)
		})
		require.NoError(t, err)
		assert.EqualValues(t, 42, n)
	})
}

// https://github.com/jackc/pgx/issues/673
func TestReleaseConnWithTxInProgress(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
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
	})
}

func TestConnPingContextSuccess(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		err := db.PingContext(context.Background())
		require.NoError(t, err)
	})
}

func TestConnPrepareContextSuccess(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		stmt, err := db.PrepareContext(context.Background(), "select now()")
		require.NoError(t, err)
		err = stmt.Close()
		require.NoError(t, err)
	})
}

func TestConnExecContextSuccess(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		_, err := db.ExecContext(context.Background(), "create temporary table exec_context_test(id serial primary key)")
		require.NoError(t, err)
	})
}

func TestConnExecContextFailureRetry(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		// we get a connection, immediately close it, and then get it back
		{
			conn, err := stdlib.AcquireConn(db)
			require.NoError(t, err)
			conn.Close(context.Background())
			stdlib.ReleaseConn(db, conn)
		}
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		_, err = conn.ExecContext(context.Background(), "select 1")
		require.EqualValues(t, driver.ErrBadConn, err)
	})
}

func TestConnQueryContextSuccess(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.QueryContext(context.Background(), "select * from generate_series(1,10) n")
		require.NoError(t, err)

		for rows.Next() {
			var n int64
			err := rows.Scan(&n)
			require.NoError(t, err)
		}
		require.NoError(t, rows.Err())
	})
}

func TestConnQueryContextFailureRetry(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		// we get a connection, immediately close it, and then get it back
		{
			conn, err := stdlib.AcquireConn(db)
			require.NoError(t, err)
			conn.Close(context.Background())
			stdlib.ReleaseConn(db, conn)
		}
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		_, err = conn.QueryContext(context.Background(), "select 1")
		require.EqualValues(t, driver.ErrBadConn, err)
	})
}

func TestRowsColumnTypeDatabaseTypeName(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("select * from generate_series(1,10) n")
		require.NoError(t, err)

		columnTypes, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Len(t, columnTypes, 1)

		if columnTypes[0].DatabaseTypeName() != "INT4" {
			t.Errorf("columnTypes[0].DatabaseTypeName() => %v, want %v", columnTypes[0].DatabaseTypeName(), "INT4")
		}

		err = rows.Close()
		require.NoError(t, err)
	})
}

func TestStmtExecContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(id int primary key)")
	require.NoError(t, err)

	stmt, err := db.Prepare("insert into t(id) values ($1::int4)")
	require.NoError(t, err)
	defer stmt.Close()

	_, err = stmt.ExecContext(context.Background(), 42)
	require.NoError(t, err)

	ensureDBValid(t, db)
}

func TestStmtExecContextCancel(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(id int primary key)")
	require.NoError(t, err)

	stmt, err := db.Prepare("insert into t(id) select $1::int4 from pg_sleep(5)")
	require.NoError(t, err)
	defer stmt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = stmt.ExecContext(ctx, 42)
	if !pgconn.Timeout(err) {
		t.Errorf("expected timeout error, got %v", err)
	}

	ensureDBValid(t, db)
}

func TestStmtQueryContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt, err := db.Prepare("select * from generate_series(1,$1::int4) n")
	require.NoError(t, err)
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background(), 5)
	require.NoError(t, err)

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
	}

	if rows.Err() != nil {
		t.Error(rows.Err())
	}

	ensureDBValid(t, db)
}

func TestRowsColumnTypes(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
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
			}, {
				Name:     "d",
				TypeName: "1266",
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
				ScanType: reflect.TypeOf(""),
			},
		}

		rows, err := db.Query("SELECT 1 AS a, text 'bar' AS bar, 1.28::numeric(9, 2) AS dec, '12:00:00'::timetz as d")
		require.NoError(t, err)

		columns, err := rows.ColumnTypes()
		require.NoError(t, err)
		assert.Len(t, columns, 4)

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
	})
}

func TestQueryLifeCycle(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("SELECT 'foo', n FROM generate_series($1::int, $2::int) n WHERE 3 = $3", 1, 10, 3)
		require.NoError(t, err)

		rowCount := int64(0)

		for rows.Next() {
			rowCount++
			var (
				s string
				n int64
			)

			err := rows.Scan(&s, &n)
			require.NoError(t, err)

			if s != "foo" {
				t.Errorf(`Expected "foo", received "%v"`, s)
			}

			if n != rowCount {
				t.Errorf("Expected %d, received %d", rowCount, n)
			}
		}
		require.NoError(t, rows.Err())

		err = rows.Close()
		require.NoError(t, err)

		rows, err = db.Query("select 1 where false")
		require.NoError(t, err)

		rowCount = int64(0)

		for rows.Next() {
			rowCount++
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 0, rowCount)

		err = rows.Close()
		require.NoError(t, err)
	})
}

// https://github.com/jackc/pgx/issues/409
func TestScanJSONIntoJSONRawMessage(t *testing.T) {
	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, db *sql.DB) {
		var msg json.RawMessage

		err := db.QueryRow("select '{}'::json").Scan(&msg)
		require.NoError(t, err)
		require.EqualValues(t, []byte("{}"), []byte(msg))
	})
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
