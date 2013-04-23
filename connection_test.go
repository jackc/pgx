package pgx

import (
	"testing"
)

var SharedConnection *Connection

func getSharedConnection() (c *Connection) {
	if SharedConnection == nil {
		var err error
		SharedConnection, err = Connect(ConnectionParameters{socket: "/private/tmp/.s.PGSQL.5432", user: "pgx_none", database: "pgx_test"})
		if err != nil {
			panic("Unable to establish connection")
		}

	}
	return SharedConnection
}

func TestConnect(t *testing.T) {
	conn, err := Connect(ConnectionParameters{socket: "/private/tmp/.s.PGSQL.5432", user: "pgx_none", database: "pgx_test"})
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

func TestConnectWithInvalidUser(t *testing.T) {
	_, err := Connect(ConnectionParameters{socket: "/private/tmp/.s.PGSQL.5432", user: "invalid_user", database: "pgx_test"})
	pgErr := err.(PgError)
	if pgErr.Code != "28000" {
		t.Fatal("Did not receive expected error when connecting with invalid user")
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	conn, err := Connect(ConnectionParameters{socket: "/private/tmp/.s.PGSQL.5432", user: "pgx_pw", password: "secret", database: "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	conn, err := Connect(ConnectionParameters{socket: "/private/tmp/.s.PGSQL.5432", user: "pgx_md5", password: "secret", database: "pgx_test"})
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

	rowCount := 0
	onDataRow := func(r *messageReader, fields []fieldDescription) error {
		rowCount++
		return nil
	}

	err := conn.SelectFunc("select generate_series(1,10)", onDataRow)
	if err != nil {
		t.Fatal("Select failed: " + err.Error())
	}
	if rowCount != 10 {
		t.Fatal("Select called onDataRow wrong number of times")
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
