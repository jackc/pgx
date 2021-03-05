package pgxpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Conn.Release is an asynchronous process that returns immediately. There is no signal when the actual work is
// completed. To test something that relies on the actual work for Conn.Release being completed we must simply wait.
// This function wraps the sleep so there is more meaning for the callers.
func waitForReleaseToComplete() {
	time.Sleep(25 * time.Millisecond)
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

func assertConfigsEqual(t *testing.T, expected, actual *pgxpool.Config, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.ConnString(), actual.ConnString(), "%s - ConnString", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)
	assert.Equalf(t, expected.BeforeAcquire == nil, actual.BeforeAcquire == nil, "%s - BeforeAcquire", testName)
	assert.Equalf(t, expected.AfterRelease == nil, actual.AfterRelease == nil, "%s - AfterRelease", testName)

	assert.Equalf(t, expected.MaxConnLifetime, actual.MaxConnLifetime, "%s - MaxConnLifetime", testName)
	assert.Equalf(t, expected.MaxConnIdleTime, actual.MaxConnIdleTime, "%s - MaxConnIdleTime", testName)
	assert.Equalf(t, expected.MaxConns, actual.MaxConns, "%s - MaxConns", testName)
	assert.Equalf(t, expected.MinConns, actual.MinConns, "%s - MinConns", testName)
	assert.Equalf(t, expected.HealthCheckPeriod, actual.HealthCheckPeriod, "%s - HealthCheckPeriod", testName)
	assert.Equalf(t, expected.LazyConnect, actual.LazyConnect, "%s - LazyConnect", testName)

	assertConnConfigsEqual(t, expected.ConnConfig, actual.ConnConfig, testName)
}

func assertConnConfigsEqual(t *testing.T, expected, actual *pgx.ConnConfig, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.Logger, actual.Logger, "%s - Logger", testName)
	assert.Equalf(t, expected.LogLevel, actual.LogLevel, "%s - LogLevel", testName)
	assert.Equalf(t, expected.ConnString(), actual.ConnString(), "%s - ConnString", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.BuildStatementCache == nil, actual.BuildStatementCache == nil, "%s - BuildStatementCache", testName)

	assert.Equalf(t, expected.PreferSimpleProtocol, actual.PreferSimpleProtocol, "%s - PreferSimpleProtocol", testName)

	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify, "%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t, expected.TLSConfig.ServerName, actual.TLSConfig.ServerName, "%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t, expected.Fallbacks[i].TLSConfig == nil, actual.Fallbacks[i].TLSConfig == nil, "%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.InsecureSkipVerify, actual.Fallbacks[i].TLSConfig.InsecureSkipVerify, "%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.ServerName, actual.Fallbacks[i].TLSConfig.ServerName, "%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}
