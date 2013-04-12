package pgx

import (
	"testing"
)

func TestConnect(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_none", "database": "pgx_test"})
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

	rows, err = conn.Query("select current_user")
	if err != nil || rows[0]["current_user"] != "pgx_none" {
		t.Error("Did not connect as specified user (pgx_none)")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithInvalidUser(t *testing.T) {
	_, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "invalid_user", "database": "pgx_test"})
	pgErr := err.(PgError)
	if pgErr.Code != "28000" {
		t.Fatal("Did not receive expected error when connecting with invalid user")
	}
}

func TestConnectWithPlainTextPassword(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_pw", "password": "secret", "database": "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestConnectWithMD5Password(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_md5", "password": "secret", "database": "pgx_test"})
	if err != nil {
		t.Fatal("Unable to establish connection: " + err.Error())
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}

func TestQuery(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432", "user": "pgx_none", "database": "pgx_test"})
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
