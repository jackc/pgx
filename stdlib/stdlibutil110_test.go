// +build go1.10

package stdlib_test

import (
	"database/sql"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

func openDB(t *testing.T) *sql.DB {
	config, err := pgx.ParseConnectionString("postgres://pgx_md5:secret@127.0.0.1:5432/pgx_test")
	if err != nil {
		t.Fatalf("pgx.ParseConnectionString failed: %v", err)
	}

	pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: config,
	})

	if err != nil {
		t.Fatalf("pgx.ParseConnectionString failed: %v", err)
	}
	return stdlib.OpenDBFromPool(pool)
}
