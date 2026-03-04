package pgx_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchQueueBulkInsert(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, `create temporary table ledger(
  id serial primary key,
  description varchar not null,
  amount int not null
);`)

		batch := &pgx.Batch{}
		qqs, err := batch.QueueBulkInsert(
			"INSERT INTO ledger (description, amount) VALUES ($1, $2)",
			0,
			"q1", 1, "q2", 2, "q3", 3,
		)
		require.NoError(t, err)
		require.Len(t, qqs, 1) // 3 rows, default batchSize=100 → single chunk
		batch.Queue("select count(*) from ledger")

		br := conn.SendBatch(ctx, batch)

		ct, err := br.Exec()
		require.NoError(t, err)
		assert.EqualValues(t, 3, ct.RowsAffected())

		var count int
		err = br.QueryRow().Scan(&count)
		require.NoError(t, err)
		assert.EqualValues(t, 3, count)

		err = br.Close()
		require.NoError(t, err)
	})
}

func TestBatchQueueBulkInsertEmptyArgs(t *testing.T) {
	t.Parallel()

	batch := &pgx.Batch{}
	qqs, err := batch.QueueBulkInsert("INSERT INTO t (c1, c2) VALUES ($1, $2)", 0)
	require.NoError(t, err)
	assert.Nil(t, qqs)
	assert.Equal(t, 0, batch.Len())
}

func TestBatchQueueBulkInsertBatchSize(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, `create temporary table ledger(
  id serial primary key,
  description varchar not null,
  amount int not null
);`)

		// 250 rows, batchSize=100 → 3 chunks: 100+100+50
		numInserts := 250
		args := make([]any, 0, numInserts*2)
		for i := range numInserts {
			args = append(args, "description", i)
		}

		batch := &pgx.Batch{}
		qqs, err := batch.QueueBulkInsert(
			"INSERT INTO ledger (description, amount) VALUES ($1, $2)",
			100,
			args...,
		)
		require.NoError(t, err)
		require.Len(t, qqs, 3)
		batch.Queue("select count(*) from ledger")

		br := conn.SendBatch(ctx, batch)

		totalInserted := int64(0)
		for range 3 {
			ct, execErr := br.Exec()
			require.NoError(t, execErr)
			totalInserted += ct.RowsAffected()
		}
		assert.EqualValues(t, numInserts, totalInserted)

		var count int
		err = br.QueryRow().Scan(&count)
		require.NoError(t, err)
		assert.EqualValues(t, numInserts, count)

		err = br.Close()
		require.NoError(t, err)
	})
}

func TestBatchQueueBulkInsertDefaultBatchSize(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		mustExec(t, conn, `create temporary table ledger(
  id serial primary key,
  description varchar not null,
  amount int not null
);`)

		// 210 rows with default batchSize=100 → 3 chunks: 100+100+10
		numInserts := 210
		args := make([]any, 0, numInserts*2)
		for i := range numInserts {
			args = append(args, "description", i)
		}

		batch := &pgx.Batch{}
		qqs, err := batch.QueueBulkInsert(
			"INSERT INTO ledger (description, amount) VALUES ($1, $2)",
			0, // use default
			args...,
		)
		require.NoError(t, err)
		require.Len(t, qqs, 3)

		br := conn.SendBatch(ctx, batch)
		for range 3 {
			_, execErr := br.Exec()
			require.NoError(t, execErr)
		}
		err = br.Close()
		require.NoError(t, err)
	})
}

func TestBatchQueueBulkInsertExceedsParameterLimit(t *testing.T) {
	t.Parallel()

	batch := &pgx.Batch{}
	// batchSize=65536, paramsPerRow=1 → 65536 > 65535
	_, err := batch.QueueBulkInsert("INSERT INTO t (c1) VALUES ($1)", 65536, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "65535")
	assert.Equal(t, 0, batch.Len())
}

func TestBatchQueueBulkInsertArgMismatch(t *testing.T) {
	t.Parallel()

	batch := &pgx.Batch{}
	// paramsPerRow=2, but 3 args → not divisible
	_, err := batch.QueueBulkInsert("INSERT INTO t (c1, c2) VALUES ($1, $2)", 0, 1, 2, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not divisible")
	assert.Equal(t, 0, batch.Len())
}

func TestBatchQueueBulkInsertOnConflict(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "ON CONFLICT behavior may differ")

		mustExec(t, conn, `create temporary table kv(
  key varchar primary key,
  val int not null
);`)
		mustExec(t, conn, "INSERT INTO kv (key, val) VALUES ('a', 1), ('b', 2)")

		// "a" and "b" already exist → ON CONFLICT DO NOTHING; only "c" is inserted
		batch := &pgx.Batch{}
		qqs, err := batch.QueueBulkInsert(
			"INSERT INTO kv (key, val) VALUES ($1, $2) ON CONFLICT DO NOTHING",
			0,
			"a", 10, "b", 20, "c", 30,
		)
		require.NoError(t, err)
		require.Len(t, qqs, 1)

		br := conn.SendBatch(ctx, batch)
		ct, err := br.Exec()
		require.NoError(t, err)
		assert.EqualValues(t, 1, ct.RowsAffected())

		err = br.Close()
		require.NoError(t, err)
	})
}
