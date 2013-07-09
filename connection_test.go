package pgx

import (
	"bytes"
	"strings"
	"testing"
)

var SharedConnection *Connection

func getSharedConnection() (c *Connection) {
	if SharedConnection == nil {
		var err error
		SharedConnection, err = Connect(*defaultConnectionParameters)
		if err != nil {
			panic("Unable to establish connection")
		}

	}
	return SharedConnection
}

func TestConnect(t *testing.T) {
	conn, err := Connect(*defaultConnectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}

	if _, present := conn.runtimeParams["server_version"]; !present {
		t.Error("Runtime parameters not stored")
	}

	if conn.pid == 0 {
		t.Error("Backend PID not stored")
	}

	if conn.secretKey == 0 {
		t.Error("Backend secret key not stored")
	}

	var rows []map[string]interface{}
	rows, err = conn.SelectRows("select current_database()")
	if err != nil || rows[0]["current_database"] != defaultConnectionParameters.Database {
		t.Errorf("Did not connect to specified database (%v)", defaultConnectionParameters.Database)
	}

	rows, err = conn.SelectRows("select current_user")
	if err != nil || rows[0]["current_user"] != defaultConnectionParameters.User {
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

	conn, err := Connect(*unixSocketConnectionParameters)
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

	conn, err := Connect(*tcpConnectionParameters)
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

	_, err := Connect(*invalidUserConnectionParameters)
	pgErr, ok := err.(PgError)
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

	conn, err := Connect(*plainPasswordConnectionParameters)
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

	conn, err := Connect(*md5ConnectionParameters)
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestExecute(t *testing.T) {
	conn := getSharedConnection()

	results, err := conn.Execute("create temporary table foo(id integer primary key);")
	if err != nil {
		t.Fatal("Execute failed: " + err.Error())
	}
	if results != "CREATE TABLE" {
		t.Error("Unexpected results from Execute")
	}

	// Accept parameters
	results, err = conn.Execute("insert into foo(id) values($1)", 1)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}
	if results != "INSERT 0 1" {
		t.Errorf("Unexpected results from Execute: %v", results)
	}

	results, err = conn.Execute("drop table foo;")
	if err != nil {
		t.Fatal("Execute failed: " + err.Error())
	}
	if results != "DROP TABLE" {
		t.Error("Unexpected results from Execute")
	}

	// Multiple statements can be executed -- last command tag is returned
	results, err = conn.Execute("create temporary table foo(id serial primary key); drop table foo;")
	if err != nil {
		t.Fatal("Execute failed: " + err.Error())
	}
	if results != "DROP TABLE" {
		t.Error("Unexpected results from Execute")
	}

	// Can execute longer SQL strings than sharedBufferSize
	results, err = conn.Execute(strings.Repeat("select 42; ", 1000))
	if err != nil {
		t.Fatal("Execute failed: " + err.Error())
	}
	if results != "SELECT 1" {
		t.Errorf("Unexpected results from Execute: %v", results)
	}
}

func TestExecuteFailure(t *testing.T) {
	conn, err := Connect(*defaultConnectionParameters)
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
	conn := getSharedConnection()

	var sum, rowCount int32
	onDataRow := func(r *DataRowReader) error {
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
	conn, err := Connect(*defaultConnectionParameters)
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

func TestSelectRows(t *testing.T) {
	conn := getSharedConnection()

	rows, err := conn.SelectRows("select $1 as name, null as position", "Jack")
	if err != nil {
		t.Fatal("Query failed")
	}

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

func TestSelectRow(t *testing.T) {
	conn := getSharedConnection()

	row, err := conn.SelectRow("select $1 as name, null as position", "Jack")
	if err != nil {
		t.Fatal("Query failed")
	}

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

	_, err = conn.SelectRow("select 'Jack' as name where 1=2")
	if _, ok := err.(NotSingleRowError); !ok {
		t.Error("No matching row should have returned NotSingleRowError")
	}

	_, err = conn.SelectRow("select * from (values ('Matthew'), ('Mark')) t")
	if _, ok := err.(NotSingleRowError); !ok {
		t.Error("Multiple matching rows should have returned NotSingleRowError")
	}
}

func TestConnectionSelectValue(t *testing.T) {
	conn := getSharedConnection()

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
	if _, ok := err.(NotSingleRowError); !ok {
		t.Error("No matching row should have returned NoRowsFoundError")
	}

	_, err = conn.SelectValue("select * from (values ('Matthew'), ('Mark')) t")
	if _, ok := err.(NotSingleRowError); !ok {
		t.Error("Multiple matching rows should have returned NotSingleRowError")
	}

	_, err = conn.SelectValue("select 'Matthew', 'Mark'")
	if _, ok := err.(UnexpectedColumnCountError); !ok {
		t.Error("Multiple columns should have returned UnexpectedColumnCountError")
	}
}

func TestSelectValues(t *testing.T) {
	conn := getSharedConnection()

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
	if _, ok := err.(UnexpectedColumnCountError); !ok {
		t.Error("Multiple columns should have returned UnexpectedColumnCountError")
	}
}

func TestPrepare(t *testing.T) {
	conn, err := Connect(*defaultConnectionParameters)
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

	if conn.SanitizeSql("select $1", bytea) != `select E'\\x000fff11'` {
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
}

func TestPrepareFailure(t *testing.T) {
	conn, err := Connect(*defaultConnectionParameters)
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
