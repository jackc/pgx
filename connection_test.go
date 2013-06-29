package pgx

import (
	"testing"
)

var SharedConnection *Connection

func getSharedConnection() (c *Connection) {
	if SharedConnection == nil {
		var err error
		SharedConnection, err = Connect(ConnectionParameters{Socket: "/private/tmp/.s.PGSQL.5432", User: "pgx_none", Database: "pgx_test"})
		if err != nil {
			panic("Unable to establish connection")
		}

	}
	return SharedConnection
}

func TestConnect(t *testing.T) {
	conn, err := Connect(ConnectionParameters{Socket: "/private/tmp/.s.PGSQL.5432", User: "pgx_none", Database: "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection")
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
	if err != nil || rows[0]["current_database"] != "pgx_test" {
		t.Error("Did not connect to specified database (pgx_text)")
	}

	rows, err = conn.SelectRows("select current_user")
	if err != nil || rows[0]["current_user"] != "pgx_none" {
		t.Error("Did not connect as specified user (pgx_none)")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithTcp(t *testing.T) {
	conn, err := Connect(ConnectionParameters{Host: "127.0.0.1", User: "pgx_md5", Password: "secret", Database: "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithInvalidUser(t *testing.T) {
	_, err := Connect(ConnectionParameters{Socket: "/private/tmp/.s.PGSQL.5432", User: "invalid_user", Database: "pgx_test"})
	pgErr := err.(PgError)
	if pgErr.Code != "28000" {
		t.Fatal("Did not receive expected error when connecting with invalid user")
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	conn, err := Connect(ConnectionParameters{Socket: "/private/tmp/.s.PGSQL.5432", User: "pgx_pw", Password: "secret", Database: "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	conn, err := Connect(ConnectionParameters{Socket: "/private/tmp/.s.PGSQL.5432", User: "pgx_md5", Password: "secret", Database: "pgx_test"})
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

	results, err := conn.Execute("create temporary table foo(id serial primary key);")
	if err != nil {
		t.Fatal("Execute failed: " + err.Error())
	}
	if results != "CREATE TABLE" {
		t.Error("Unexpected results from Execute")
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
}

func TestSelectFunc(t *testing.T) {
	conn := getSharedConnection()

	var sum, rowCount int32
	onDataRow := func(r *DataRowReader) error {
		rowCount++
		sum += r.ReadValue().(int32)
		return nil
	}

	err := conn.SelectFunc("select generate_series(1,10)", onDataRow)
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

func TestSelectRows(t *testing.T) {
	conn := getSharedConnection()

	rows, err := conn.SelectRows("select 'Jack' as name, null as position")
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

	row, err := conn.SelectRow("select 'Jack' as name, null as position")
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

	test := func(sql string, expected interface{}) {
		v, err := conn.SelectValue(sql)
		if err != nil {
			t.Errorf("%v while running %v", err, sql)
		} else {
			if v != expected {
				t.Errorf("Expected: %#v Received: %#v", expected, v)
			}
		}
	}

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
}

func TestSelectValues(t *testing.T) {
	conn := getSharedConnection()

	test := func(sql string, expected []interface{}) {
		values, err := conn.SelectValues(sql)
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

	test("select * from (values ('Matthew'), ('Mark'), ('Luke'), ('John')) t", []interface{}{"Matthew", "Mark", "Luke", "John"})
	test("select * from (values ('Matthew'), (null)) t", []interface{}{"Matthew", nil})
	test("select * from (values (1::int4), (2::int4), (null), (3::int4)) t", []interface{}{int32(1), int32(2), nil, int32(3)})
}
