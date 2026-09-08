package pgx_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestConnSendBatchBeginTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, "create temporary table batch_tx(id integer primary key)")

		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{})
		batch.Queue("insert into batch_tx(id) values ($1)", 1)

		br := conn.SendBatch(ctx, batch)
		tx := br.Tx()
		require.NotNil(t, tx)
		defer tx.Rollback(ctx)

		require.NoError(t, br.Close())
		require.Same(t, tx, br.Tx())
		require.EqualValues(t, 'T', conn.PgConn().TxStatus())

		_, err := tx.Exec(ctx, "insert into batch_tx(id) values ($1)", 2)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)

		var n int64
		require.NoError(t, conn.QueryRow(ctx, "select count(*) from batch_tx").Scan(&n))
		require.EqualValues(t, 2, n)
	})
}

func TestConnSendBatchBeginTxIsFirstResult(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{})
		batch.Queue("select 1")

		br := conn.SendBatch(ctx, batch)
		defer br.Tx().Rollback(ctx)

		ct, err := br.Exec()
		require.NoError(t, err)
		require.Equal(t, "BEGIN", ct.String())

		var n int32
		require.NoError(t, br.QueryRow().Scan(&n))
		require.EqualValues(t, 1, n)

		require.NoError(t, br.Close())
	})
}

func TestConnSendBatchBeginTxOptions(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support all transaction modes")

		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		batch.Queue("select current_setting('transaction_isolation'), current_setting('transaction_read_only')")

		br := conn.SendBatch(ctx, batch)
		defer br.Tx().Rollback(ctx)

		_, err := br.Exec()
		require.NoError(t, err)

		var isoLevel, readOnly string
		require.NoError(t, br.QueryRow().Scan(&isoLevel, &readOnly))
		require.NoError(t, br.Close())
		require.Equal(t, string(pgx.RepeatableRead), isoLevel)
		require.Equal(t, "on", readOnly)
	})
}

func TestConnSendBatchBeginTxCommitQuery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support COMMIT AND CHAIN")

		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{CommitQuery: "commit and chain"})
		batch.Queue("select 1")

		br := conn.SendBatch(ctx, batch)
		require.NoError(t, br.Close())
		require.NoError(t, br.Tx().Commit(ctx))

		// COMMIT AND CHAIN leaves the connection in a new transaction, which proves the configured commit query ran.
		require.EqualValues(t, 'T', conn.PgConn().TxStatus())
		mustExec(t, conn, "rollback")
	})
}

func TestConnSendBatchBeginTxQueryError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{})
		batch.Queue("select 1/0")

		br := conn.SendBatch(ctx, batch)
		tx := br.Tx()

		var pgErr *pgconn.PgError
		require.ErrorAs(t, br.Close(), &pgErr)
		require.EqualValues(t, 'E', conn.PgConn().TxStatus())

		require.NoError(t, tx.Rollback(ctx))
		require.EqualValues(t, 'I', conn.PgConn().TxStatus())
	})
}

func TestConnSendBatchBeginTxBeginError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{BeginQuery: "begin garbage"})
		batch.Queue("select 1")

		br := conn.SendBatch(ctx, batch)
		tx := br.Tx()
		require.NotNil(t, tx)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, br.Close(), &pgErr)
		require.EqualValues(t, 'I', conn.PgConn().TxStatus())

		// No transaction was begun, so the Tx is closed instead of sending a rollback that has nothing to roll back.
		require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)
		require.ErrorIs(t, tx.Commit(ctx), pgx.ErrTxClosed)

		var n int32
		require.NoError(t, conn.QueryRow(ctx, "select 1").Scan(&n))
	})
}

type errQueryRewriter struct{}

func (errQueryRewriter) RewriteQuery(ctx context.Context, conn *pgx.Conn, sql string, args []any) (string, []any, error) {
	return "", nil, errors.New("rewrite failed")
}

func TestConnSendBatchBeginTxNotSent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		batch := &pgx.Batch{}
		batch.BeginTx(pgx.TxOptions{})
		batch.Queue("select 1", errQueryRewriter{})

		br := conn.SendBatch(ctx, batch)
		tx := br.Tx()
		require.NotNil(t, tx)

		require.ErrorContains(t, br.Close(), "rewrite failed")
		require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)

		var n int32
		require.NoError(t, conn.QueryRow(ctx, "select 1").Scan(&n))
	})
}

func TestConnSendBatchTxIsNilWithoutBeginTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	batch := &pgx.Batch{}
	batch.Queue("select 1")
	br := conn.SendBatch(ctx, batch)
	require.Nil(t, br.Tx())
	require.NoError(t, br.Close())

	require.Nil(t, conn.SendBatch(ctx, &pgx.Batch{}).Tx())
}

func TestBatchBeginTxMustBeFirst(t *testing.T) {
	t.Parallel()

	batch := &pgx.Batch{}
	batch.Queue("select 1")
	require.Panics(t, func() { batch.BeginTx(pgx.TxOptions{}) })

	batch = &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	require.Panics(t, func() { batch.BeginTx(pgx.TxOptions{}) })
}

func TestTxSendBatchRejectsBeginTx(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1")

	br := tx.SendBatch(ctx, batch)
	require.Error(t, br.Close())
	require.ErrorIs(t, br.Tx().Rollback(ctx), pgx.ErrTxClosed)

	nested, err := tx.Begin(ctx)
	require.NoError(t, err)
	br = nested.SendBatch(ctx, batch)
	require.Error(t, br.Close())
	require.ErrorIs(t, br.Tx().Rollback(ctx), pgx.ErrTxClosed)

	// The outer transaction is unaffected.
	var n int32
	require.NoError(t, tx.QueryRow(ctx, "select 1").Scan(&n))
	require.EqualValues(t, 'T', conn.PgConn().TxStatus())

	// A batch that begins a transaction is refused on a closed transaction the same way.
	require.NoError(t, tx.Rollback(ctx))
	br = tx.SendBatch(ctx, batch)
	require.ErrorIs(t, br.Close(), pgx.ErrTxClosed)
	require.ErrorIs(t, br.Tx().Commit(ctx), pgx.ErrTxClosed)
}

func TestFailedBatchResults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	failure := errors.New("failure")

	batch := &pgx.Batch{}
	batch.Queue("select 1")
	br := pgx.FailedBatchResults(batch, failure)
	_, err := br.Exec()
	require.ErrorIs(t, err, failure)
	_, err = br.Query()
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, br.QueryRow().Scan(new(int32)), failure)
	require.ErrorIs(t, br.Close(), failure)
	require.ErrorIs(t, br.Close(), failure)
	require.Nil(t, br.Tx())

	batch = &pgx.Batch{}
	batch.BeginTx(pgx.TxOptions{})
	batch.Queue("select 1")
	br = pgx.FailedBatchResults(batch, failure)
	require.ErrorIs(t, br.Close(), failure)

	tx := br.Tx()
	require.NotNil(t, tx)
	require.ErrorIs(t, tx.Rollback(ctx), pgx.ErrTxClosed)
	require.ErrorIs(t, tx.Commit(ctx), pgx.ErrTxClosed)
	lo := tx.LargeObjects()
	_, err = lo.Create(ctx, 0)
	require.ErrorIs(t, err, pgx.ErrTxClosed)
}
