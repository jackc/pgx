package pgxpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Conn.Release is an asynchronous process that returns immediately. There is no signal when the actual work is
// completed. To test something that relies on the actual work for Conn.Release being completed we must simply wait.
// This function wraps the sleep so there is more meaning for the callers.
func waitForReleaseToComplete() {
	time.Sleep(5 * time.Millisecond)
}

type execer interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
}

func testExec(t *testing.T, db execer) {
	results, err := db.Exec(context.Background(), "set time zone 'America/Chicago'")
	require.NoError(t, err)
	assert.EqualValues(t, "SET", results)
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

func testQuery(t *testing.T, db queryer) {
	var sum, rowCount int32

	rows, err := db.Query(context.Background(), "select generate_series(1,$1)", 10)
	require.NoError(t, err)

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	assert.NoError(t, rows.Err())
	assert.Equal(t, int32(10), rowCount)
	assert.Equal(t, int32(55), sum)
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func testQueryRow(t *testing.T, db queryRower) {
	var what, who string
	err := db.QueryRow(context.Background(), "select 'hello', $1::text", "world").Scan(&what, &who)
	assert.NoError(t, err)
	assert.Equal(t, "hello", what)
	assert.Equal(t, "world", who)
}

type sendBatcher interface {
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

func testSendBatch(t *testing.T, db sendBatcher) {
	batch := &pgx.Batch{}
	batch.Queue("select 1")
	batch.Queue("select 2")

	br := db.SendBatch(context.Background(), batch)

	var err error
	var n int32
	err = br.QueryRow().Scan(&n)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, n)

	err = br.QueryRow().Scan(&n)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, n)

	err = br.Close()
	assert.NoError(t, err)
}

type copyFromer interface {
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

func testCopyFrom(t *testing.T, db interface {
	execer
	queryer
	copyFromer
}) {
	_, err := db.Exec(context.Background(), `create temporary table foo(a int2, b int4, c int8, d varchar, e text, f date, g timestamptz)`)
	require.NoError(t, err)

	tzedTime := time.Date(2010, 2, 3, 4, 5, 6, 0, time.Local)

	inputRows := [][]interface{}{
		{int16(0), int32(1), int64(2), "abc", "efg", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), tzedTime},
		{nil, nil, nil, nil, nil, nil, nil},
	}

	copyCount, err := db.CopyFrom(context.Background(), pgx.Identifier{"foo"}, []string{"a", "b", "c", "d", "e", "f", "g"}, pgx.CopyFromRows(inputRows))
	assert.NoError(t, err)
	assert.EqualValues(t, len(inputRows), copyCount)

	rows, err := db.Query(context.Background(), "select * from foo")
	assert.NoError(t, err)

	var outputRows [][]interface{}
	for rows.Next() {
		row, err := rows.Values()
		if err != nil {
			t.Errorf("Unexpected error for rows.Values(): %v", err)
		}
		outputRows = append(outputRows, row)
	}

	assert.NoError(t, rows.Err())
	assert.Equal(t, inputRows, outputRows)
}
