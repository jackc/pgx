package pgx_test

import (
	"github.com/JackC/pgx"
	"io"
	"testing"
)

func mustConnect(t testing.TB, config pgx.ConnConfig) *pgx.Conn {
	conn, err := pgx.Connect(config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return conn
}

func closeConn(t testing.TB, conn *pgx.Conn) {
	err := conn.Close()
	if err != nil {
		t.Fatalf("conn.Close unexpectedly failed: %v", err)
	}
}

func mustPrepare(t testing.TB, conn *pgx.Conn, name, sql string) {
	if _, err := conn.Prepare(name, sql); err != nil {
		t.Fatalf("Could not prepare %v: %v", name, err)
	}
}

func mustExecute(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (commandTag pgx.CommandTag) {
	var err error
	if commandTag, err = conn.Execute(sql, arguments...); err != nil {
		t.Fatalf("Execute unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectRow(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (row map[string]interface{}) {
	var err error
	if row, err = conn.SelectRow(sql, arguments...); err != nil {
		t.Fatalf("SelectRow unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectRows(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (rows []map[string]interface{}) {
	var err error
	if rows, err = conn.SelectRows(sql, arguments...); err != nil {
		t.Fatalf("SelectRows unexpected failed with %v: %v", sql, err)
	}
	return
}

func mustSelectValue(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (value interface{}) {
	var err error
	if value, err = conn.SelectValue(sql, arguments...); err != nil {
		t.Fatalf("SelectValue unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectValueTo(t testing.TB, conn *pgx.Conn, w io.Writer, sql string, arguments ...interface{}) {
	if err := conn.SelectValueTo(w, sql, arguments...); err != nil {
		t.Fatalf("SelectValueTo unexpectedly failed with %v: %v", sql, err)
	}
}
