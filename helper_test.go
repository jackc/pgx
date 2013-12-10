package pgx_test

import (
	"github.com/JackC/pgx"
	"testing"
)

var sharedConnection *pgx.Connection

func getSharedConnection(t testing.TB) (c *pgx.Connection) {
	if sharedConnection == nil || !sharedConnection.IsAlive() {
		var err error
		sharedConnection, err = pgx.Connect(*defaultConnectionParameters)
		if err != nil {
			t.Fatalf("Unable to establish connection: %v", err)
		}

	}
	return sharedConnection
}

func mustPrepare(t testing.TB, conn *pgx.Connection, name, sql string) {
	if err := conn.Prepare(name, sql); err != nil {
		t.Fatalf("Could not prepare %v: %v", name, err)
	}
}

func mustExecute(t testing.TB, conn *pgx.Connection, sql string, arguments ...interface{}) (commandTag string) {
	var err error
	if commandTag, err = conn.Execute(sql, arguments...); err != nil {
		t.Fatalf("Execute unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectRow(t testing.TB, conn *pgx.Connection, sql string, arguments ...interface{}) (row map[string]interface{}) {
	var err error
	if row, err = conn.SelectRow(sql, arguments...); err != nil {
		t.Fatalf("SelectRow unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectRows(t testing.TB, conn *pgx.Connection, sql string, arguments ...interface{}) (rows []map[string]interface{}) {
	var err error
	if rows, err = conn.SelectRows(sql, arguments...); err != nil {
		t.Fatalf("SelectRows unexpected failed with %v: %v", sql, err)
	}
	return
}

func mustSelectValue(t testing.TB, conn *pgx.Connection, sql string, arguments ...interface{}) (value interface{}) {
	var err error
	if value, err = conn.SelectValue(sql, arguments...); err != nil {
		t.Fatalf("SelectValue unexpectedly failed with %v: %v", sql, err)
	}
	return
}
