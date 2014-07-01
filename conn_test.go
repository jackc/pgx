package pgx_test

import (
	"bytes"
	"fmt"
	"github.com/jackc/pgx"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	t.Parallel()

	conn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	if _, present := conn.RuntimeParams["server_version"]; !present {
		t.Error("Runtime parameters not stored")
	}

	if conn.Pid == 0 {
		t.Error("Backend PID not stored")
	}

	if conn.SecretKey == 0 {
		t.Error("Backend secret key not stored")
	}

	currentDB, err := conn.SelectValue("select current_database()")
	if err != nil || currentDB != defaultConnConfig.Database {
		t.Errorf("Did not connect to specified database (%v)", defaultConnConfig.Database)
	}

	if user := mustSelectValue(t, conn, "select current_user"); user != defaultConnConfig.User {
		t.Errorf("Did not connect as specified user (%v)", defaultConnConfig.User)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithUnixSocketDirectory(t *testing.T) {
	t.Parallel()

	// /.s.PGSQL.5432
	if unixSocketConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*unixSocketConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithUnixSocketFile(t *testing.T) {
	t.Parallel()

	if unixSocketConnConfig == nil {
		return
	}

	connParams := *unixSocketConnConfig
	connParams.Host = connParams.Host + "/.s.PGSQL.5432"
	conn, err := pgx.Connect(connParams)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTcp(t *testing.T) {
	t.Parallel()

	if tcpConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*tcpConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTLS(t *testing.T) {
	t.Parallel()

	if tlsConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*tlsConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithInvalidUser(t *testing.T) {
	t.Parallel()

	if invalidUserConnConfig == nil {
		return
	}

	_, err := pgx.Connect(*invalidUserConnConfig)
	pgErr, ok := err.(pgx.PgError)
	if !ok {
		t.Fatalf("Expected to receive a PgError with code 28000, instead received: %v", err)
	}
	if pgErr.Code != "28000" && pgErr.Code != "28P01" {
		t.Fatalf("Expected to receive a PgError with code 28000 or 28P01, instead received: %v", pgErr)
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	t.Parallel()

	if plainPasswordConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*plainPasswordConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	t.Parallel()

	if md5ConnConfig == nil {
		return
	}

	conn, err := pgx.Connect(*md5ConnConfig)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestParseURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		url        string
		connParams pgx.ConnConfig
	}{
		{
			url: "postgres://jack:secret@localhost:5432/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
			},
		},
		{
			url: "postgresql://jack:secret@localhost:5432/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
			},
		},
		{
			url: "postgres://jack@localhost:5432/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
			},
		},
		{
			url: "postgres://jack@localhost/mydb",
			connParams: pgx.ConnConfig{
				User:     "jack",
				Host:     "localhost",
				Database: "mydb",
			},
		},
	}

	for i, tt := range tests {
		connParams, err := pgx.ParseURI(tt.url)
		if err != nil {
			t.Errorf("%d. Unexpected error from pgx.ParseURL(%q) => %v", i, tt.url, err)
			continue
		}

		if connParams != tt.connParams {
			t.Errorf("%d. expected %#v got %#v", i, tt.connParams, connParams)
		}
	}
}

func TestExec(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if results := mustExec(t, conn, "create temporary table foo(id integer primary key);"); results != "CREATE TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Accept parameters
	if results := mustExec(t, conn, "insert into foo(id) values($1)", 1); results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}

	if results := mustExec(t, conn, "drop table foo;"); results != "DROP TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Multiple statements can be executed -- last command tag is returned
	if results := mustExec(t, conn, "create temporary table foo(id serial primary key); drop table foo;"); results != "DROP TABLE" {
		t.Error("Unexpected results from Exec")
	}

	// Can execute longer SQL strings than sharedBufferSize
	if results := mustExec(t, conn, strings.Repeat("select 42; ", 1000)); results != "SELECT 1" {
		t.Errorf("Unexpected results from Exec: %v", results)
	}
}

func TestExecFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Exec("select;"); err == nil {
		t.Fatal("Expected SQL syntax error")
	}

	if _, err := conn.SelectValue("select 1"); err != nil {
		t.Fatalf("Exec failure appears to have broken connection: %v", err)
	}
}

func TestConnQuery(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var sum, rowCount int32

	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	defer rows.Close()

	for rows.NextRow() {
		var rr pgx.RowReader
		sum += rr.ReadInt32(rows)
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, conn *pgx.Conn) {
	var sum, rowCount int32

	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	defer qr.Close()

	for qr.NextRow() {
		var rr pgx.RowReader
		sum += rr.ReadInt32(qr)
		rowCount++
	}

	if qr.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

// Test that a connection stays valid when query results are closed early
func TestConnQueryCloseEarly(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Immediately close query without reading any rows
	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	qr.Close()

	ensureConnValid(t, conn)

	// Read partial response then close
	qr, err = conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := qr.NextRow()
	if !ok {
		t.Fatal("qr.NextRow terminated early")
	}

	var rr pgx.RowReader
	if n := rr.ReadInt32(qr); n != 1 {
		t.Fatalf("Expected 1 from first row, but got %v", n)
	}

	qr.Close()

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadWrongTypeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Read a single value incorrectly
	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	rowsRead := 0

	for qr.NextRow() {
		var rr pgx.RowReader
		rr.ReadDate(qr)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if qr.Err() == nil {
		t.Fatal("Expected QueryResult to have an error after an improper read but it didn't")
	}

	// Read too many values
	qr, err = conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	rowsRead = 0

	for qr.NextRow() {
		var rr pgx.RowReader
		rr.ReadInt32(qr)
		rr.ReadInt32(qr)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if qr.Err() == nil {
		t.Fatal("Expected QueryResult to have an error after an improper read but it didn't")
	}

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadTooManyValues(t *testing.T) {
	// t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Read too many values
	qr, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	rowsRead := 0

	for qr.NextRow() {
		var rr pgx.RowReader
		rr.ReadInt32(qr)
		rr.ReadInt32(qr)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if qr.Err() == nil {
		t.Fatal("Expected QueryResult to have an error after an improper read but it didn't")
	}

	ensureConnValid(t, conn)
}

func TestConnectionSelectValue(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	test := func(sql string, expected interface{}, arguments ...interface{}) {
		v, err := conn.SelectValue(sql, arguments...)
		if err != nil {
			t.Errorf("%v while running %v", err, sql)
		} else {
			if v != expected {
				t.Errorf("Expected: %#v Received: %#v", expected, v)
			}
		}
	}

	fmt.Println("Starting test")
	test("select $1", "foo", "foo")
	test("select 'foo'", "foo")
	test("select true", true)
	test("select false", false)
	test("select 1::int2", int16(1))
	test("select 1::int4", int32(1))
	test("select 1::int8", int64(1))
	test("select 1.23::float4", float32(1.23))
	test("select 1.23::float8", float64(1.23))

	_, err := conn.SelectValue("select 'Jack' as name where 1=2")
	if _, ok := err.(pgx.NotSingleRowError); !ok {
		t.Error("No matching row should have returned NoRowsFoundError")
	}

	_, err = conn.SelectValue("select * from (values ('Matthew'), ('Mark')) t")
	if _, ok := err.(pgx.NotSingleRowError); !ok {
		t.Error("Multiple matching rows should have returned NotSingleRowError")
	}

	_, err = conn.SelectValue("select 'Matthew', 'Mark'")
	if _, ok := err.(pgx.UnexpectedColumnCountError); !ok {
		t.Error("Multiple columns should have returned UnexpectedColumnCountError")
	}
}

func TestConnectionSelectValueTo(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var err error

	var buf bytes.Buffer
	if err := conn.SelectValueTo(&buf, "select '[1,2,3,4,5]'::json"); err != nil {
		t.Fatalf("SelectValueTo failed: %v", err)
	}
	if bytes.Compare(buf.Bytes(), []byte("[1,2,3,4,5]")) != 0 {
		t.Fatalf("SelectValueTo did not write expected data: %v", string(buf.Bytes()))
	}

	// NotSingleRowError
	err = conn.SelectValueTo(&buf, "select * from (values ('Matthew'), ('Mark'), ('Luke')) t")
	if _, ok := err.(pgx.NotSingleRowError); !ok {
		t.Fatalf("Multiple matching rows should have returned NotSingleRowError: %#v", err)
	}
	if conn.IsAlive() {
		mustSelectValue(t, conn, "select 1") // ensure it really is alive and usable
	} else {
		t.Fatal("SelectValueTo NotSingleRowError should not have killed connection")
	}

	// UnexpectedColumnCountError
	err = conn.SelectValueTo(&buf, "select * from (values ('Matthew', 'Mark', 'Luke')) t")
	if _, ok := err.(pgx.UnexpectedColumnCountError); !ok {
		t.Fatalf("Multiple matching rows should have returned UnexpectedColumnCountError: %#v", err)
	}
	if conn.IsAlive() {
		mustSelectValue(t, conn, "select 1") // ensure it really is alive and usable
	} else {
		t.Fatal("SelectValueTo UnexpectedColumnCountError should not have killed connection")
	}

	// Null
	err = conn.SelectValueTo(&buf, "select null")
	if err == nil || err.Error() != "SelectValueTo cannot handle null" {
		t.Fatalf("Expected null error: %#v", err)
	}
	if conn.IsAlive() {
		mustSelectValue(t, conn, "select 1") // ensure it really is alive and usable
	} else {
		t.Fatal("SelectValueTo null error should not have killed connection")
	}

}

func TestPrepare(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	testTranscode := func(sql string, value interface{}) {
		if _, err := conn.Prepare("testTranscode", sql); err != nil {
			t.Errorf("Unable to prepare statement: %v", err)
			return
		}
		defer func() {
			err := conn.Deallocate("testTranscode")
			if err != nil {
				t.Errorf("Deallocate failed: %v", err)
			}
		}()

		result, err := conn.SelectValue("testTranscode", value)
		if err != nil {
			t.Errorf("%v while running %v", err, "testTranscode")
		} else {
			if result != value {
				t.Errorf("Expected: %#v Received: %#v", value, result)
			}
		}
	}

	// Test parameter encoding and decoding for simple supported data types
	testTranscode("select $1::varchar", "foo")
	testTranscode("select $1::text", "foo")
	testTranscode("select $1::int2", int16(1))
	testTranscode("select $1::int4", int32(1))
	testTranscode("select $1::int8", int64(1))
	testTranscode("select $1::float4", float32(1.23))
	testTranscode("select $1::float8", float64(1.23))
	testTranscode("select $1::boolean", true)

	// Ensure that unknown types are just treated as strings
	testTranscode("select $1::point", "(0,0)")

	if _, err := conn.Prepare("testByteSliceTranscode", "select $1::bytea"); err != nil {
		t.Errorf("Unable to prepare statement: %v", err)
		return
	}
	defer func() {
		err := conn.Deallocate("testByteSliceTranscode")
		if err != nil {
			t.Errorf("Deallocate failed: %v", err)
		}
	}()

	bytea := make([]byte, 4)
	bytea[0] = 0   // 0x00
	bytea[1] = 15  // 0x0F
	bytea[2] = 255 // 0xFF
	bytea[3] = 17  // 0x11

	if sql, err := conn.SanitizeSql("select $1", bytea); err != nil {
		t.Errorf("Error sanitizing []byte: %v", err)
	} else if sql != `select E'\\x000fff11'` {
		t.Error("Failed to sanitize []byte")
	}

	result, err := conn.SelectValue("testByteSliceTranscode", bytea)
	if err != nil {
		t.Errorf("%v while running %v", err, "testByteSliceTranscode")
	} else {
		if bytes.Compare(result.([]byte), bytea) != 0 {
			t.Errorf("Expected: %#v Received: %#v", bytea, result)
		}
	}

	mustExec(t, conn, "create temporary table foo(id serial)")
	if _, err = conn.Prepare("deleteFoo", "delete from foo"); err != nil {
		t.Fatalf("Unable to prepare delete: %v", err)
	}
}

func TestPrepareFailure(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	if _, err := conn.Prepare("badSQL", "select foo"); err == nil {
		t.Fatal("Prepare should have failed with syntax error")
	}

	if _, err := conn.SelectValue("select 1"); err != nil {
		t.Fatalf("Prepare failure appears to have broken connection: %v", err)
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

	var n interface{}
	n = mustSelectValue(t, conn, "select count(*) from foo")
	if n.(int64) != 1 {
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
	n = mustSelectValue(t, conn, "select count(*) from foo")
	if n.(int64) != 0 {
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
	n = mustSelectValue(t, conn, "select count(*) from foo")
	if n.(int64) != 0 {
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

	n = mustSelectValue(t, conn, "select count(*) from foo")
	if n.(int64) != 0 {
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

		n = mustSelectValue(t, conn, "select count(*) from foo")
		if n.(int64) != 0 {
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
			if level := mustSelectValue(t, conn, "select current_setting('transaction_isolation')"); level != iso {
				t.Errorf("Expected to be in isolation level %v but was %v", iso, level)
			}
			return true
		})
		if err != nil {
			t.Fatalf("Unexpected transaction failure: %v", err)
		}
	}
}

func TestListenNotify(t *testing.T) {
	t.Parallel()

	listener := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, listener)

	if err := listener.Listen("chat"); err != nil {
		t.Fatalf("Unable to start listening: %v", err)
	}

	notifier := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, notifier)

	mustExec(t, notifier, "notify chat")

	// when notification is waiting on the socket to be read
	notification, err := listener.WaitForNotification(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when notification has already been read during previous query
	mustExec(t, notifier, "notify chat")
	mustSelectValue(t, listener, "select 1")
	notification, err = listener.WaitForNotification(0)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when timeout occurs
	notification, err = listener.WaitForNotification(time.Millisecond)
	if err != pgx.NotificationTimeoutError {
		t.Errorf("WaitForNotification returned the wrong kind of error: %v", err)
	}
	if notification != nil {
		t.Errorf("WaitForNotification returned an unexpected notification: %v", notification)
	}

	// listener can listen again after a timeout
	mustExec(t, notifier, "notify chat")
	notification, err = listener.WaitForNotification(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}
}

func TestFatalRxError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := conn.SelectValue("select 1, pg_sleep(10)")
		if err == nil {
			t.Fatal("Expected error but none occurred")
		}
	}()

	otherConn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	if _, err := otherConn.Exec("select pg_terminate_backend($1)", conn.Pid); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	wg.Wait()

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}

func TestFatalTxError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	otherConn, err := pgx.Connect(*defaultConnConfig)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	_, err = otherConn.Exec("select pg_terminate_backend($1)", conn.Pid)
	if err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	_, err = conn.SelectValue("select 1")
	if err == nil {
		t.Fatal("Expected error but none occurred")
	}

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}

func TestCommandTag(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		commandTag   pgx.CommandTag
		rowsAffected int64
	}{
		{commandTag: "INSERT 0 5", rowsAffected: 5},
		{commandTag: "UPDATE 0", rowsAffected: 0},
		{commandTag: "UPDATE 1", rowsAffected: 1},
		{commandTag: "DELETE 0", rowsAffected: 0},
		{commandTag: "DELETE 1", rowsAffected: 1},
		{commandTag: "CREATE TABLE", rowsAffected: 0},
		{commandTag: "ALTER TABLE", rowsAffected: 0},
		{commandTag: "DROP TABLE", rowsAffected: 0},
	}

	for i, tt := range tests {
		actual := tt.commandTag.RowsAffected()
		if tt.rowsAffected != actual {
			t.Errorf(`%d. "%s" should have affected %d rows but it was %d`, i, tt.commandTag, tt.rowsAffected, actual)
		}
	}
}
