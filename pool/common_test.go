package pool_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx"
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
	results, err := db.Exec(context.Background(), "create table foo(id integer primary key);")
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE", string(results))

	results, err = db.Exec(context.Background(), "insert into foo(id) values($1)", 1)
	require.NoError(t, err)
	assert.Equal(t, "INSERT 0 1", string(results))

	results, err = db.Exec(context.Background(), "drop table foo;")
	require.NoError(t, err)
	assert.Equal(t, "DROP TABLE", string(results))
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
	err := db.QueryRow(context.Background(), "select 'hello', $1", "world").Scan(&what, &who)
	assert.NoError(t, err)
	assert.Equal(t, "hello", what)
	assert.Equal(t, "world", who)
}
