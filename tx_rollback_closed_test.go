package pgx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// When a transaction query's context is canceled mid-flight, the low-level
// pgconn is torn down but the outer pgx.Tx state is left open. The next
// Rollback should honor its documented contract and return an error matching
// ErrTxClosed, rather than leaking the low-level "conn closed" error.
//
// Regression test for jackc/pgx#2557.
func TestTransactionRollbackAfterCanceledQueryReturnsErrTxClosed(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)

	// Sleep longer than the context timeout so the query is canceled and the
	// underlying connection is closed out from under the still-open Tx.
	_, _ = tx.Exec(ctx, "SELECT pg_sleep(1)")
	require.True(t, conn.IsClosed(), "expected the connection to be closed after the canceled query")

	// The first rollback after the connection died must report the tx as
	// closed (ErrTxClosed), not surface the raw "conn closed" error.
	err = tx.Rollback(context.Background())
	require.ErrorIs(t, err, pgx.ErrTxClosed)
}
