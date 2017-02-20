package pgtype

import (
	"database/sql"
	"os"
	"testing"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	_ "github.com/lib/pq"
)

func mustConnectAll(t testing.TB) []QueryRowCloser {
	return []QueryRowCloser{
		mustConnectPgx(t),
		mustConnectDatabaseSQL(t, "github.com/lib/pq"),
		mustConnectDatabaseSQL(t, "github.com/jackc/pgx/stdlib"),
	}
}

func mustCloseAll(t testing.TB, conns []QueryRowCloser) {
	for _, conn := range conns {
		err := conn.Close()
		if err != nil {
			t.Error(err)
		}
	}
}

func mustConnectPgx(t testing.TB) QueryRowCloser {
	config, err := pgx.ParseURI(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Fatal(err)
	}

	return &PgxConn{conn: conn}
}

func mustClose(t testing.TB, conn interface {
	Close() error
}) {
	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func mustConnectDatabaseSQL(t testing.TB, driverName string) QueryRowCloser {
	var sqlDriverName string
	switch driverName {
	case "github.com/lib/pq":
		sqlDriverName = "postgres"
	case "github.com/jackc/pgx/stdlib":
		sqlDriverName = "pgx"
	default:
		t.Fatalf("Unknown driver %v", driverName)
	}

	db, err := sql.Open(sqlDriverName, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	return &DatabaseSQLConn{db: db, name: driverName}
}

type QueryRowScanner interface {
	Scan(dest ...interface{}) error
}

type QueryRowCloser interface {
	QueryRow(query string, args ...interface{}) QueryRowScanner
	Close() error
	DriverName() string
}

type DatabaseSQLConn struct {
	db   *sql.DB
	name string
}

func (c *DatabaseSQLConn) QueryRow(query string, args ...interface{}) QueryRowScanner {
	return c.db.QueryRow(query, args...)
}

func (c *DatabaseSQLConn) Close() error {
	return c.db.Close()
}

func (c *DatabaseSQLConn) DriverName() string {
	return c.name
}

type PgxConn struct {
	conn *pgx.Conn
}

func (c *PgxConn) QueryRow(query string, args ...interface{}) QueryRowScanner {
	return c.conn.QueryRow(query, args...)
}

func (c *PgxConn) Close() error {
	return c.conn.Close()
}

func (c *PgxConn) DriverName() string {
	return "github.com/jackc/pgx"
}

// Test scan lib/pq
// Test encode lib/pq
// Test scan pgx/stdlib
// Test encode pgx/stdlib
// Test scan pgx binary
// Test scan pgx text
// Test encode pgx

func TestInt32BoxScan(t *testing.T) {
	conns := mustConnectAll(t)
	defer mustCloseAll(t, conns)

	tests := []struct {
		name   string
		sql    string
		args   []interface{}
		err    error
		result Int32Box
	}{
		{
			name:   "Scan",
			sql:    "select 42",
			args:   []interface{}{},
			err:    nil,
			result: Int32Box{Status: Present, Value2: 42},
		},
		{
			name:   "Encode",
			sql:    "select $1::int4",
			args:   []interface{}{&Int32Box{Status: Present, Value2: 42}},
			err:    nil,
			result: Int32Box{Status: Present, Value2: 42},
		},
	}

	for _, conn := range conns {
		for _, tt := range tests {
			var n Int32Box
			err := conn.QueryRow(tt.sql, tt.args...).Scan(&n)
			if err != tt.err {
				t.Errorf("%s %s: %v", conn.DriverName(), tt.name, err)
			}

			if n.Status != tt.result.Status {
				t.Errorf("%s %s: expected Status %v, got %v", conn.DriverName(), tt.name, tt.result.Status, n.Status)
			}
			if n.Value2 != tt.result.Value2 {
				t.Errorf("%s %s: expected Value %v, got %v", conn.DriverName(), tt.name, tt.result.Value2, n.Value2)
			}
		}
	}
}

func TestStringBoxScan(t *testing.T) {
	conns := mustConnectAll(t)
	defer mustCloseAll(t, conns)

	for _, conn := range conns {
		var n StringBox
		err := conn.QueryRow("select 'Hello, world'").Scan(&n)
		if err != nil {
			t.Errorf("%s: %v", conn.DriverName(), err)
		}

		if n.Status != Present {
			t.Errorf("%s: expected Status %v, got %v", conn.DriverName(), Present, n.Status)
		}
		if n.Value != "Hello, world" {
			t.Errorf("%s: expected Value %v, got %v", "Hello, world", conn.DriverName(), n.Value)
		}
	}
}
