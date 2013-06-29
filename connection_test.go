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

	var rows []map[string]string
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

	value, presence := rows[0]["position"]
	if value != "" {
		t.Error("Should have received empty string for null")
	}
	if presence != false {
		t.Error("Null value shouldn't have been present in map")
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

	value, presence := row["position"]
	if value != "" {
		t.Error("Should have received empty string for null")
	}
	if presence != false {
		t.Error("Null value shouldn't have been present in map")
	}

	row, err = conn.SelectRow("select 'Jack' as name where 1=2")
	if row != nil {
		t.Error("No matching row should have returned nil")
	}
	if err != nil {
		t.Fatal("Query failed")
	}
}

func TestConnectionSelectValue(t *testing.T) {
	conn := getSharedConnection()
	var v interface{}
	var err error

	v, err = conn.SelectValue("select null")
	if err != nil {
		t.Errorf("Unable to select null: %v", err)
	} else {
		if v != nil {
			t.Errorf("Expected: nil, recieved: %v", v)
		}
	}

	v, err = conn.SelectValue("select 'foo'")
	if err != nil {
		t.Errorf("Unable to select string: %v", err)
	} else {
		s, ok := v.(string)
		if !(ok && s == "foo") {
			t.Errorf("Expected: foo, recieved: %#v", v)
		}
	}

	v, err = conn.SelectValue("select true")
	if err != nil {
		t.Errorf("Unable to select bool: %#v", err)
	} else {
		s, ok := v.(bool)
		if !(ok && s == true) {
			t.Errorf("Expected true, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select false")
	if err != nil {
		t.Errorf("Unable to select bool: %v", err)
	} else {
		s, ok := v.(bool)
		if !(ok && s == false) {
			t.Errorf("Expected false, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1::int2")
	if err != nil {
		t.Errorf("Unable to select int2: %v", err)
	} else {
		s, ok := v.(int16)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1::int4")
	if err != nil {
		t.Errorf("Unable to select int4: %v", err)
	} else {
		s, ok := v.(int32)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1::int8")
	if err != nil {
		t.Errorf("Unable to select int8: %#v", err)
	} else {
		s, ok := v.(int64)
		if !(ok && s == 1) {
			t.Errorf("Expected 1, received: %#v", v)
		}
	}

	v, err = conn.SelectValue("select 1.23::float4")
	if err != nil {
		t.Errorf("Unable to select float4: %#v", err)
	} else {
		s, ok := v.(float32)
		if !(ok && s == float32(1.23)) {
			t.Errorf("Expected 1.23, received: %#v", v)
		}
	}
}

func TestSelectValues(t *testing.T) {
	conn := getSharedConnection()
	var values []interface{}
	var err error

	values, err = conn.SelectValues("select * from (values ('Matthew'), ('Mark'), ('Luke'), ('John')) t")
	if err != nil {
		t.Fatalf("Unable to select all strings: %v", err)
	}
	if values[0].(string) != "Matthew" || values[1].(string) != "Mark" || values[2].(string) != "Luke" || values[3].(string) != "John" {
		t.Error("Received incorrect strings")
	}

	values, err = conn.SelectValues("select * from (values ('Matthew'), (null)) t")
	if err != nil {
		t.Fatalf("Unable to select values including a null: %v", err)
	}
	if values[0].(string) != "Matthew" || values[1] != nil {
		t.Error("Received incorrect strings")
	}
}
