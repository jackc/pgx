package pgx_test

import (
	"bytes"
	"fmt"
	"github.com/JackC/pgx"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
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

	var rows []map[string]interface{}
	rows, err = conn.SelectRows("select current_database()")
	if err != nil || rows[0]["current_database"] != defaultConnectionParameters.Database {
		t.Errorf("Did not connect to specified database (%v)", defaultConnectionParameters.Database)
	}

	if user := mustSelectValue(t, conn, "select current_user"); user != defaultConnectionParameters.User {
		t.Errorf("Did not connect as specified user (%v)", defaultConnectionParameters.User)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithUnixSocket(t *testing.T) {
	if unixSocketConnectionParameters == nil {
		return
	}

	conn, err := pgx.Connect(*unixSocketConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTcp(t *testing.T) {
	if tcpConnectionParameters == nil {
		return
	}

	conn, err := pgx.Connect(*tcpConnectionParameters)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTLS(t *testing.T) {
	if tlsConnectionParameters == nil {
		return
	}

	conn, err := pgx.Connect(*tlsConnectionParameters)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithInvalidUser(t *testing.T) {
	if invalidUserConnectionParameters == nil {
		return
	}

	_, err := pgx.Connect(*invalidUserConnectionParameters)
	pgErr, ok := err.(pgx.PgError)
	if !ok {
		t.Fatalf("Expected to receive a PgError with code 28000, instead received: %v", err)
	}
	if pgErr.Code != "28000" && pgErr.Code != "28P01" {
		t.Fatalf("Expected to receive a PgError with code 28000 or 28P01, instead received: %v", pgErr)
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	if plainPasswordConnectionParameters == nil {
		return
	}

	conn, err := pgx.Connect(*plainPasswordConnectionParameters)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	if md5ConnectionParameters == nil {
		return
	}

	conn, err := pgx.Connect(*md5ConnectionParameters)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestExecute(t *testing.T) {
	conn := getSharedConnection(t)

	if results := mustExecute(t, conn, "create temporary table foo(id integer primary key);"); results != "CREATE TABLE" {
		t.Error("Unexpected results from Execute")
	}

	// Accept parameters
	if results := mustExecute(t, conn, "insert into foo(id) values($1)", 1); results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Execute: %v", results)
	}

	if results := mustExecute(t, conn, "drop table foo;"); results != "DROP TABLE" {
		t.Error("Unexpected results from Execute")
	}

	// Multiple statements can be executed -- last command tag is returned
	if results := mustExecute(t, conn, "create temporary table foo(id serial primary key); drop table foo;"); results != "DROP TABLE" {
		t.Error("Unexpected results from Execute")
	}

	// Can execute longer SQL strings than sharedBufferSize
	if results := mustExecute(t, conn, strings.Repeat("select 42; ", 1000)); results != "SELECT 1" {
		t.Errorf("Unexpected results from Execute: %v", results)
	}
}

func TestExecuteFailure(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Execute("select;"); err == nil {
		t.Fatal("Expected SQL syntax error")
	}

	if _, err := conn.SelectValue("select 1"); err != nil {
		t.Fatalf("Execute failure appears to have broken connection: %v", err)
	}
}

func TestSelectFunc(t *testing.T) {
	conn := getSharedConnection(t)

	var sum, rowCount int32
	onDataRow := func(r *pgx.DataRowReader) error {
		rowCount++
		sum += r.ReadValue().(int32)
		return nil
	}

	err := conn.SelectFunc("select generate_series(1,$1)", onDataRow, 10)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

func TestSelectFuncFailure(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	// using SelectValue as it delegates to SelectFunc and is easier to work with
	if _, err := conn.SelectValue("select;"); err == nil {
		t.Fatal("Expected SQL syntax error")
	}

	if _, err := conn.SelectValue("select 1"); err != nil {
		t.Fatalf("SelectFunc failure appears to have broken connection: %v", err)
	}
}

func Example_connectionSelectFunc() {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	onDataRow := func(r *pgx.DataRowReader) error {
		fmt.Println(r.ReadValue())
		return nil
	}

	err = conn.SelectFunc("select generate_series(1,$1)", onDataRow, 5)
	if err != nil {
		fmt.Println(err)
	}
	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func TestSelectRows(t *testing.T) {
	conn := getSharedConnection(t)

	rows := mustSelectRows(t, conn, "select $1 as name, null as position", "Jack")

	if len(rows) != 1 {
		t.Fatal("Received wrong number of rows")
	}

	if rows[0]["name"] != "Jack" {
		t.Error("Received incorrect name")
	}

	if value, presence := rows[0]["position"]; presence {
		if value != nil {
			t.Error("Should have received nil for null")
		}
	} else {
		t.Error("Null value should have been present in map as nil")
	}
}

func Example_connectionSelectRows() {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	var rows []map[string]interface{}
	if rows, err = conn.SelectRows("select generate_series(1,$1) as number", 5); err != nil {
		fmt.Printf("Error selecting rows: %v", err)
		return
	}
	for _, r := range rows {
		fmt.Println(r["number"])
	}
	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func TestSelectRow(t *testing.T) {
	conn := getSharedConnection(t)

	row := mustSelectRow(t, conn, "select $1 as name, null as position", "Jack")
	if row["name"] != "Jack" {
		t.Error("Received incorrect name")
	}

	if value, presence := row["position"]; presence {
		if value != nil {
			t.Error("Should have received nil for null")
		}
	} else {
		t.Error("Null value should have been present in map as nil")
	}

	_, err := conn.SelectRow("select 'Jack' as name where 1=2")
	if _, ok := err.(pgx.NotSingleRowError); !ok {
		t.Error("No matching row should have returned NotSingleRowError")
	}

	_, err = conn.SelectRow("select * from (values ('Matthew'), ('Mark')) t")
	if _, ok := err.(pgx.NotSingleRowError); !ok {
		t.Error("Multiple matching rows should have returned NotSingleRowError")
	}
}

func TestConnectionSelectValue(t *testing.T) {
	conn := getSharedConnection(t)

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
	conn := getSharedConnection(t)
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

func TestSelectValues(t *testing.T) {
	conn := getSharedConnection(t)

	test := func(sql string, expected []interface{}, arguments ...interface{}) {
		values, err := conn.SelectValues(sql, arguments...)
		if err != nil {
			t.Errorf("%v while running %v", err, sql)
			return
		}
		if len(values) != len(expected) {
			t.Errorf("Expected: %#v Received: %#v", expected, values)
			return
		}
		for i := 0; i < len(values); i++ {
			if values[i] != expected[i] {
				t.Errorf("Expected: %#v Received: %#v", expected, values)
				return
			}
		}
	}

	test("select * from (values ($1)) t", []interface{}{"Matthew"}, "Matthew")
	test("select * from (values ('Matthew'), ('Mark'), ('Luke'), ('John')) t", []interface{}{"Matthew", "Mark", "Luke", "John"})
	test("select * from (values ('Matthew'), (null)) t", []interface{}{"Matthew", nil})
	test("select * from (values (1::int4), (2::int4), (null), (3::int4)) t", []interface{}{int32(1), int32(2), nil, int32(3)})

	_, err := conn.SelectValues("select 'Matthew', 'Mark'")
	if _, ok := err.(pgx.UnexpectedColumnCountError); !ok {
		t.Error("Multiple columns should have returned UnexpectedColumnCountError")
	}
}

func TestPrepare(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	testTranscode := func(sql string, value interface{}) {
		if err = conn.Prepare("testTranscode", sql); err != nil {
			t.Errorf("Unable to prepare statement: %v", err)
			return
		}
		defer func() {
			err := conn.Deallocate("testTranscode")
			if err != nil {
				t.Errorf("Deallocate failed: %v", err)
			}
		}()

		var result interface{}
		result, err = conn.SelectValue("testTranscode", value)
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

	if err = conn.Prepare("testByteSliceTranscode", "select $1::bytea"); err != nil {
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
	var result interface{}
	result, err = conn.SelectValue("testByteSliceTranscode", bytea)
	if err != nil {
		t.Errorf("%v while running %v", err, "testByteSliceTranscode")
	} else {
		if bytes.Compare(result.([]byte), bytea) != 0 {
			t.Errorf("Expected: %#v Received: %#v", bytea, result)
		}
	}

	mustExecute(t, conn, "create temporary table foo(id serial)")
	if err = conn.Prepare("deleteFoo", "delete from foo"); err != nil {
		t.Fatalf("Unable to prepare delete: %v", err)
	}
}

func TestPrepareFailure(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	if err = conn.Prepare("badSQL", "select foo"); err == nil {
		t.Fatal("Prepare should have failed with syntax error")
	}

	if _, err = conn.SelectValue("select 1"); err != nil {
		t.Fatalf("Prepare failure appears to have broken connection: %v", err)
	}
}

func TestTransaction(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	createSql := `
		create temporary table foo(
			id integer,
			unique (id) initially deferred
		);
	`

	if _, err := conn.Execute(createSql); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	var committed bool

	// Transaction happy path -- it executes function and commits
	committed, err = conn.Transaction(func() bool {
		mustExecute(t, conn, "insert into foo(id) values (1)")
		return true
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: ", err)
	}
	if !committed {
		t.Fatal("Transaction was not committed")
	}

	var n interface{}
	n = mustSelectValue(t, conn, "select count(*) from foo")
	if n.(int64) != 1 {
		t.Fatalf("Did not receive correct number of rows: %v", n)
	}

	mustExecute(t, conn, "truncate foo")

	// It rolls back when passed function returns false
	committed, err = conn.Transaction(func() bool {
		mustExecute(t, conn, "insert into foo(id) values (1)")
		return false
	})
	if err != nil {
		t.Fatalf("Transaction unexpectedly failed: ", err)
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
		mustExecute(t, conn, "insert into foo(id) values (1)")
		if _, err := conn.Execute("invalid"); err == nil {
			t.Fatal("Execute was supposed to error but didn't")
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
		mustExecute(t, conn, "insert into foo(id) values (1)")
		mustExecute(t, conn, "insert into foo(id) values (1)")
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

	// when something in transaction panicks
	func() {
		defer func() {
			recover()
		}()

		committed, err = conn.Transaction(func() bool {
			mustExecute(t, conn, "insert into foo(id) values (1)")
			panic("stop!")
		})

		n = mustSelectValue(t, conn, "select count(*) from foo")
		if n.(int64) != 0 {
			t.Fatalf("Did not receive correct number of rows: %v", n)
		}
	}()
}

func TestTransactionIso(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	isoLevels := []string{"serializable", "repeatable read", "read committed", "read uncommitted"}
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
	listener, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer listener.Close()

	if err := listener.Listen("chat"); err != nil {
		t.Fatalf("Unable to start listening: %v", err)
	}

	notifier := getSharedConnection(t)
	mustExecute(t, notifier, "notify chat")

	// when notification is waiting on the socket to be read
	notification, err := listener.WaitForNotification(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error on WaitForNotification: %v", err)
	}
	if notification.Channel != "chat" {
		t.Errorf("Did not receive notification on expected channel: %v", notification.Channel)
	}

	// when notification has already been read during previous query
	mustExecute(t, notifier, "notify chat")
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
	if _, ok := err.(*net.OpError); !ok {
		t.Errorf("WaitForNotification returned the wrong kind of error: %v", err)
	}
	if notification != nil {
		t.Errorf("WaitForNotification returned an unexpected notification: %v", notification)
	}
}

func TestFatalRxError(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := conn.SelectValue("select 1, pg_sleep(10)")
		if err == nil {
			t.Fatal("Expected error but none occurred")
		}
	}()

	otherConn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	if _, err := otherConn.Execute("select pg_terminate_backend($1)", conn.Pid); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	wg.Wait()

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}

func TestFatalTxError(t *testing.T) {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close()

	otherConn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	defer otherConn.Close()

	if _, err := otherConn.Execute("select pg_terminate_backend($1)", conn.Pid); err != nil {
		t.Fatalf("Unable to kill backend PostgreSQL process: %v", err)
	}

	if _, err := conn.SelectValue("select 1"); err == nil {
		t.Fatal("Expected error but none occurred")
	}

	if conn.IsAlive() {
		t.Fatal("Connection should not be live but was")
	}
}
