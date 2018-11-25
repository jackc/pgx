// +build go1.10

package stdlib_test

import (
	"database/sql"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

func openDB(t *testing.T) *sql.DB {
	config, err := pgx.ParseConnectionString(testConnStr)
	if err != nil {
		t.Fatalf("pgx.ParseConnectionString failed: %v", err)
	}

	return stdlib.OpenDB(config)
}
