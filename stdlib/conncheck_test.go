// +build go1.10,!windows

package stdlib_test

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStaleConnectionChecks(t *testing.T) {
	db, err := sql.Open("pgx", os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	_, err = db.Exec("SET SESSION idle_in_transaction_session_timeout = 100; BEGIN;")
	require.NoError(t, err)

	// wait for PostgreSQL to close our connection
	time.Sleep(150 * time.Millisecond)

	err = db.Ping()
	require.NoError(t, err)

	closeDB(t, db)
}
