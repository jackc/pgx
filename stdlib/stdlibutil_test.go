// +build !go1.10

package stdlib_test

import (
	"database/sql"
	"os"
	"testing"
)

// this file contains utility functions for tests that differ between versions.
func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	return db
}
