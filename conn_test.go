package pqx

import (
	"testing"
)

func TestConnect(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "database": "pgx_test"})
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
	rows, err = conn.Query("select current_database()")
	if err != nil || rows[0]["current_database"] != "pgx_test" {
		t.Error("Did not connect to specified database (pgx_text)")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestQuery(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432"})
	if err != nil {
		t.Fatal("Unable to establish connection")
	}

	var rows []map[string]string
	rows, err = conn.Query("select 'Jack' as name")
	if err != nil {
		t.Fatal("Query failed")
	}

	if len(rows) != 1 {
		t.Fatal("Received wrong number of rows")
	}

	if rows[0]["name"] != "Jack" {
		t.Fatal("Received incorrect name")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}
