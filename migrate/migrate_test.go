package migrate_test

import (
	"fmt"
	"github.com/JackC/pgx"
	"github.com/JackC/pgx/migrate"
	"testing"
)

var versionTable string = "schema_version"

func clearMigrate(t *testing.T, conn *pgx.Connection) {
	tables := []string{versionTable, "t", "t1", "t2"}
	for _, table := range tables {
		mustExecute(t, conn, "drop table if exists "+table)
	}
}

func TestNewMigrator(t *testing.T) {
	conn := mustConnect(t, defaultConnectionParameters)
	clearMigrate(t, conn)

	var m *migrate.Migrator
	var err error
	m, err = migrate.NewMigrator(conn, versionTable)
	if err != nil {
		t.Fatalf("Unable to create migrator: %v", err)
	}

	schemaVersionExists := mustSelectValue(t,
		conn,
		"select exists(select 1 from information_schema.tables where table_catalog=$1 and table_name=$2)",
		defaultConnectionParameters.Database,
		versionTable).(bool)

	if !schemaVersionExists {
		t.Fatalf("NewMigrator did not create %v table", versionTable)
	}

	m, err = migrate.NewMigrator(conn, versionTable)
	if err != nil {
		t.Fatalf("NewMigrator failed when %v table already exists: %v", versionTable, err)
	}

	var initialVersion int32
	initialVersion, err = m.GetCurrentVersion()
	if err != nil {
		t.Fatalf("Failed to get current version: %v", err)
	}
	if initialVersion != 0 {
		t.Fatalf("Expected initial version to be 0. but it was %v", initialVersion)
	}
}

func TestAppendMigration(t *testing.T) {
	conn := mustConnect(t, defaultConnectionParameters)
	clearMigrate(t, conn)
	m := mustCreateMigrator(t, conn)

	name := "Update t"
	sql := "update t set c=1"
	m.AppendMigration(name, sql)

	if len(m.Migrations) != 1 {
		t.Fatal("Expected AppendMigration to add a migration but it didn't")
	}
	if m.Migrations[0].Name != name {
		t.Fatalf("expected first migration Name to be %v, but it was %v", name, m.Migrations[0].Name)
	}
	if m.Migrations[0].SQL != sql {
		t.Fatalf("expected first migration SQL to be %v, but it was %v", sql, m.Migrations[0].SQL)
	}
}

func TestPendingMigrations(t *testing.T) {
	conn := mustConnect(t, defaultConnectionParameters)
	clearMigrate(t, conn)
	m := mustCreateMigrator(t, conn)

	m.AppendMigration("update t", "update t set c=1")
	m.AppendMigration("update z", "update z set c=1")

	mustExecute(t, conn, "update "+versionTable+" set version=1")

	pending, err := m.PendingMigrations()
	if err != nil {
		t.Fatalf("Unexpected error while getting pending migrations: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("Expected 1 pending migrations but there was %v", len(pending))
	}
	if pending[0] != m.Migrations[1] {
		t.Fatal("Did not include expected migration as pending")
	}

	// Higher version than we know about
	mustExecute(t, conn, "update "+versionTable+" set version=999")
	_, err = m.PendingMigrations()
	if _, ok := err.(migrate.BadVersionError); !ok {
		t.Fatalf("Expected BadVersionError but received: %#v", err)
	}

	// Lower version than is possible
	mustExecute(t, conn, "update "+versionTable+" set version=-1")
	_, err = m.PendingMigrations()
	if _, ok := err.(migrate.BadVersionError); !ok {
		t.Fatalf("Expected BadVersionError but received: %#v", err)
	}
}

func TestMigrate(t *testing.T) {
	conn := mustConnect(t, defaultConnectionParameters)
	clearMigrate(t, conn)
	m := mustCreateMigrator(t, conn)

	m.AppendMigration("create t", "create table t(name text primary key)")

	if err := m.Migrate(); err != nil {
		t.Fatalf("Unexpected error running Migrate: %v", err)
	}

	if pending, err := m.PendingMigrations(); err != nil {
		t.Fatalf("Unexpected error while getting pending migrations: %v", err)
	} else if len(pending) != 0 {
		t.Fatalf("Migrate did not do all migrations: %v pending", len(pending))
	}

	// Now test the OnStart callback and the Migrate when some are already done
	var onStartCallCount int
	m.OnStart = func(*migrate.Migration) {
		onStartCallCount++
	}
	m.AppendMigration("create t2", "create table t2(name text primary key)")

	if err := m.Migrate(); err != nil {
		t.Fatalf("Unexpected error running Migrate: %v", err)
	}

	if pending, err := m.PendingMigrations(); err != nil {
		t.Fatalf("Unexpected error while getting pending migrations: %v", err)
	} else if len(pending) != 0 {
		t.Fatalf("Migrate did not do all migrations: %v pending", len(pending))
	}

	if onStartCallCount != 1 {
		t.Fatalf("Expected OnStart to be called 1 time, but it was called %v times", onStartCallCount)
	}

}

func Example_OnStartMigrationProgressLogging() {
	conn, err := pgx.Connect(*defaultConnectionParameters)
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	// Clear any previous runs
	if _, err = conn.Execute("drop table if exists schema_version"); err != nil {
		fmt.Printf("Unable to drop schema_version table: %v", err)
		return
	}

	var m *migrate.Migrator
	m, err = migrate.NewMigrator(conn, "schema_version")
	if err != nil {
		fmt.Printf("Unable to create migrator: %v", err)
		return
	}

	m.OnStart = func(migration *migrate.Migration) {
		fmt.Printf("Executing: %v", migration.Name)
	}

	m.AppendMigration("create a table", "create temporary table foo(id serial primary key)")

	if err = m.Migrate(); err != nil {
		fmt.Printf("Unexpected failure migrating: %v", err)
		return
	}
	// Output:
	// Executing: create a table
}
