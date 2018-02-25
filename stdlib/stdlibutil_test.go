// +build !go1.10

package stdlib_test

import (
	"database/sql"
	"testing"
)

// this file contains utility functions for tests that differ between versions.
func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", "postgres://pgx_md5:secret@127.0.0.1:5432/pgx_test")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	return db
}
