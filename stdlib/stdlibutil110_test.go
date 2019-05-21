// +build go1.10

package stdlib_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
)

func openDB(t *testing.T) *sql.DB {
	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatalf("pgx.ParseConnectionString failed: %v", err)
	}

	return stdlib.OpenDB(*config)
}
