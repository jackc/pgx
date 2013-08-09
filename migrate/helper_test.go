package migrate_test

import (
	"github.com/JackC/pgx"
	"github.com/JackC/pgx/migrate"
)

type test interface {
	Fatalf(format string, args ...interface{})
}

func mustConnect(t test, connectionParameters *pgx.ConnectionParameters) (conn *pgx.Connection) {
	var err error
	conn, err = pgx.Connect(*connectionParameters)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return
}

func mustCreateMigrator(t test, conn *pgx.Connection) (m *migrate.Migrator) {
	var err error
	m, err = migrate.NewMigrator(conn, versionTable)
	if err != nil {
		t.Fatalf("Unable to create migrator: %v", err)
	}
	return
}

func mustExecute(t test, conn *pgx.Connection, sql string, arguments ...interface{}) (commandTag string) {
	var err error
	if commandTag, err = conn.Execute(sql, arguments...); err != nil {
		t.Fatalf("Execute unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectValue(t test, conn *pgx.Connection, sql string, arguments ...interface{}) (value interface{}) {
	var err error
	if value, err = conn.SelectValue(sql, arguments...); err != nil {
		t.Fatalf("SelectValue unexpectedly failed with %v: %v", sql, err)
	}
	return
}
