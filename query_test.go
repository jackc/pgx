package pgx_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnQueryScan(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var sum, rowCount int32

	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", rows.Err())
	}

	assert.Equal(t, "SELECT 10", rows.CommandTag().String())

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

func TestConnQueryRowsFieldDescriptionsBeforeNext(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "select 'hello' as msg")
	require.NoError(t, err)
	defer rows.Close()

	require.Len(t, rows.FieldDescriptions(), 1)
	assert.Equal(t, "msg", rows.FieldDescriptions()[0].Name)
}

func TestConnQueryWithoutResultSetCommandTag(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "create temporary table t (id serial);")
	assert.NoError(t, err)
	rows.Close()
	assert.NoError(t, rows.Err())
	assert.Equal(t, "CREATE TABLE", rows.CommandTag().String())
}

func TestConnQueryScanWithManyColumns(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	columnCount := 1000
	sql := "select "
	for i := 0; i < columnCount; i++ {
		if i > 0 {
			sql += ","
		}
		sql += fmt.Sprintf(" %d", i)
	}
	sql += " from generate_series(1,5)"

	dest := make([]int, columnCount)

	var rowCount int

	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		destPtrs := make([]any, columnCount)
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			t.Fatalf("rows.Scan failed: %v", err)
		}
		rowCount++

		for i := range dest {
			if dest[i] != i {
				t.Errorf("dest[%d] => %d, want %d", i, dest[i], i)
			}
		}
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", rows.Err())
	}

	if rowCount != 5 {
		t.Errorf("rowCount => %d, want %d", rowCount, 5)
	}
}

func TestConnQueryValues(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var rowCount int32

	rows, err := conn.Query(context.Background(), "select 'foo'::text, 'bar'::varchar, n, null, n from generate_series(1,$1) n", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		rowCount++

		values, err := rows.Values()
		require.NoError(t, err)
		require.Len(t, values, 5)
		assert.Equal(t, "foo", values[0])
		assert.Equal(t, "bar", values[1])
		assert.EqualValues(t, rowCount, values[2])
		assert.Nil(t, values[3])
		assert.EqualValues(t, rowCount, values[4])
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", rows.Err())
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
}

// https://github.com/jackc/pgx/issues/666
func TestConnQueryValuesWhenUnableToDecode(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// Note that this relies on pgtype.Record not supporting the text protocol. This seems safe as it is impossible to
	// decode the text protocol because unlike the binary protocol there is no way to determine the OIDs of the elements.
	rows, err := conn.Query(context.Background(), "select (array[1::oid], null)", pgx.QueryResultFormats{pgx.TextFormatCode})
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	values, err := rows.Values()
	require.NoError(t, err)
	require.Equal(t, "({1},)", values[0])
}

func TestConnQueryValuesWithUnregisteredOID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "create type fruit as enum('orange', 'apple', 'pear')")
	require.NoError(t, err)

	rows, err := conn.Query(context.Background(), "select 'orange'::fruit")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	values, err := rows.Values()
	require.NoError(t, err)
	require.Equal(t, "orange", values[0])
}

func TestConnQueryArgsAndScanWithUnregisteredOID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, "create type fruit as enum('orange', 'apple', 'pear')")
		require.NoError(t, err)

		var result string
		err = conn.QueryRow(ctx, "select $1::fruit", "orange").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "orange", result)
	})
}

// https://github.com/jackc/pgx/issues/478
func TestConnQueryReadRowMultipleTimes(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var rowCount int32

	rows, err := conn.Query(context.Background(), "select 'foo'::text, 'bar'::varchar, n, null, n from generate_series(1,$1) n", 10)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		rowCount++

		for i := 0; i < 2; i++ {
			values, err := rows.Values()
			require.NoError(t, err)
			require.Len(t, values, 5)
			require.Equal(t, "foo", values[0])
			require.Equal(t, "bar", values[1])
			require.EqualValues(t, rowCount, values[2])
			require.Nil(t, values[3])
			require.EqualValues(t, rowCount, values[4])

			var a, b string
			var c int32
			var d pgtype.Text
			var e int32

			err = rows.Scan(&a, &b, &c, &d, &e)
			require.NoError(t, err)
			require.Equal(t, "foo", a)
			require.Equal(t, "bar", b)
			require.Equal(t, rowCount, c)
			require.False(t, d.Valid)
			require.Equal(t, rowCount, e)
		}
	}

	require.NoError(t, rows.Err())
	require.Equal(t, int32(10), rowCount)
}

// https://github.com/jackc/pgx/issues/228
func TestRowsScanDoesNotAllowScanningBinaryFormatValuesIntoString(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server does not support point type")

	var s string

	err := conn.QueryRow(context.Background(), "select point(1,2)").Scan(&s)
	if err == nil || !(strings.Contains(err.Error(), "cannot scan point (OID 600) in binary format into *string")) {
		t.Fatalf("Expected Scan to fail to scan binary value into string but: %v", err)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryRawValues(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var rowCount int32

	rows, err := conn.Query(
		context.Background(),
		"select 'foo'::text, 'bar'::varchar, n, null, n from generate_series(1,$1) n",
		pgx.QueryExecModeSimpleProtocol,
		10,
	)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		rowCount++

		rawValues := rows.RawValues()
		assert.Len(t, rawValues, 5)
		assert.Equal(t, "foo", string(rawValues[0]))
		assert.Equal(t, "bar", string(rawValues[1]))
		assert.Equal(t, strconv.FormatInt(int64(rowCount), 10), string(rawValues[2]))
		assert.Nil(t, rawValues[3])
		assert.Equal(t, strconv.FormatInt(int64(rowCount), 10), string(rawValues[4]))
	}

	require.NoError(t, rows.Err())
	assert.EqualValues(t, 10, rowCount)
}

// Test that a connection stays valid when query results are closed early
func TestConnQueryCloseEarly(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// Immediately close query without reading any rows
	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	rows.Close()

	ensureConnValid(t, conn)

	// Read partial response then close
	rows, err = conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var n int32
	rows.Scan(&n)
	if n != 1 {
		t.Fatalf("Expected 1 from first row, but got %v", n)
	}

	rows.Close()

	ensureConnValid(t, conn)
}

func TestConnQueryCloseEarlyWithErrorOnWire(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "select 1/(10-n) from generate_series(1,10) n")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	assert.False(t, pgconn.SafeToRetry(err))
	rows.Close()

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadWrongTypeError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// Read a single value incorrectly
	rows, err := conn.Query(context.Background(), "select n::int4 from generate_series(1,$1) n", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	rowsRead := 0

	for rows.Next() {
		var t time.Time
		rows.Scan(&t)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if rows.Err() == nil {
		t.Fatal("Expected Rows to have an error after an improper read but it didn't")
	}

	if rows.Err().Error() != "can't scan into dest[0] (col: n): cannot scan int4 (OID 23) in binary format into *time.Time" {
		t.Fatalf("Expected different Rows.Err(): %v", rows.Err())
	}

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadTooManyValues(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// Read too many values
	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	rowsRead := 0

	for rows.Next() {
		var n, m int32
		rows.Scan(&n, &m)
		rowsRead++
	}

	if rowsRead != 1 {
		t.Fatalf("Expected error to cause only 1 row to be read, but %d were read", rowsRead)
	}

	if rows.Err() == nil {
		t.Fatal("Expected Rows to have an error after an improper read but it didn't")
	}

	ensureConnValid(t, conn)
}

func TestConnQueryScanIgnoreColumn(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "select 1::int8, 2::int8, 3::int8")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var n, m int64
	err = rows.Scan(&n, nil, &m)
	if err != nil {
		t.Fatalf("rows.Scan failed: %v", err)
	}
	rows.Close()

	if n != 1 {
		t.Errorf("Expected n to equal 1, but it was %d", n)
	}

	if m != 3 {
		t.Errorf("Expected n to equal 3, but it was %d", m)
	}

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/570
func TestConnQueryDeferredError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")

	mustExec(t, conn, `create temporary table t (
	id text primary key,
	n int not null,
	unique (n) deferrable initially deferred
);

insert into t (id, n) values ('a', 1), ('b', 2), ('c', 3);`)

	rows, err := conn.Query(context.Background(), `update t set n=n+1 where id='b' returning *`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var n int32
		err = rows.Scan(&id, &n)
		if err != nil {
			t.Fatal(err)
		}
	}

	if rows.Err() == nil {
		t.Fatal("expected error 23505 but got none")
	}

	if err, ok := rows.Err().(*pgconn.PgError); !ok || err.Code != "23505" {
		t.Fatalf("expected error 23505, got %v", err)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryErrorWhileReturningRows(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server uses numeric instead of int")

	for i := 0; i < 100; i++ {
		func() {
			sql := `select 42 / (random() * 20)::integer from generate_series(1,100000)`

			rows, err := conn.Query(context.Background(), sql)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			for rows.Next() {
				var n int32
				if err := rows.Scan(&n); err != nil {
					t.Fatalf("Row scan failed: %v", err)
				}
			}

			if _, ok := rows.Err().(*pgconn.PgError); !ok {
				t.Fatalf("Expected pgconn.PgError, got %v", rows.Err())
			}

			ensureConnValid(t, conn)
		}()
	}

}

func TestQueryEncodeError(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "select $1::integer", "wrong")
	if err != nil {
		t.Errorf("conn.Query failure: %v", err)
	}
	assert.False(t, pgconn.SafeToRetry(err))
	defer rows.Close()

	rows.Next()

	if rows.Err() == nil {
		t.Error("Expected rows.Err() to return error, but it didn't")
	}
	if !strings.Contains(rows.Err().Error(), "SQLSTATE 22P02") {
		t.Error("Expected rows.Err() to return different error:", rows.Err())
	}
}

func TestQueryRowCoreTypes(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	type allTypes struct {
		s   string
		f32 float32
		f64 float64
		b   bool
		t   time.Time
		oid uint32
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []any
		scanArgs  []any
		expected  allTypes
	}{
		{"select $1::text", []any{"Jack"}, []any{&actual.s}, allTypes{s: "Jack"}},
		{"select $1::float4", []any{float32(1.23)}, []any{&actual.f32}, allTypes{f32: 1.23}},
		{"select $1::float8", []any{float64(1.23)}, []any{&actual.f64}, allTypes{f64: 1.23}},
		{"select $1::bool", []any{true}, []any{&actual.b}, allTypes{b: true}},
		{"select $1::timestamptz", []any{time.Unix(123, 5000)}, []any{&actual.t}, allTypes{t: time.Unix(123, 5000)}},
		{"select $1::timestamp", []any{time.Date(2010, 1, 2, 3, 4, 5, 0, time.UTC)}, []any{&actual.t}, allTypes{t: time.Date(2010, 1, 2, 3, 4, 5, 0, time.UTC)}},
		{"select $1::date", []any{time.Date(1987, 1, 2, 0, 0, 0, 0, time.UTC)}, []any{&actual.t}, allTypes{t: time.Date(1987, 1, 2, 0, 0, 0, 0, time.UTC)}},
		{"select $1::oid", []any{uint32(42)}, []any{&actual.oid}, allTypes{oid: 42}},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(context.Background(), tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, tt.sql, tt.queryArgs)
		}

		if actual.s != tt.expected.s || actual.f32 != tt.expected.f32 || actual.b != tt.expected.b || !actual.t.Equal(tt.expected.t) || actual.oid != tt.expected.oid {
			t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArgs -> %v)", i, tt.expected, actual, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)

		// Check that Scan errors when a core type is null
		err = conn.QueryRow(context.Background(), tt.sql, nil).Scan(tt.scanArgs...)
		if err == nil {
			t.Errorf("%d. Expected null to cause error, but it didn't (sql -> %v)", i, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowCoreIntegerEncoding(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	type allTypes struct {
		i16 int16
		i32 int32
		i64 int64
	}

	var actual, zero allTypes

	successfulEncodeTests := []struct {
		sql      string
		queryArg any
		scanArg  any
		expected allTypes
	}{
		// Check any integer type where value is within int2 range can be encoded
		{"select $1::int2", int(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", int8(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", int16(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", int32(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", int64(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", uint(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", uint8(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", uint16(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", uint32(42), &actual.i16, allTypes{i16: 42}},
		{"select $1::int2", uint64(42), &actual.i16, allTypes{i16: 42}},

		// Check any integer type where value is within int4 range can be encoded
		{"select $1::int4", int(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", int8(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", int16(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", int32(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", int64(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", uint(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", uint8(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", uint16(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", uint32(42), &actual.i32, allTypes{i32: 42}},
		{"select $1::int4", uint64(42), &actual.i32, allTypes{i32: 42}},

		// Check any integer type where value is within int8 range can be encoded
		{"select $1::int8", int(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", int8(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", int16(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", int32(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", int64(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", uint(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", uint8(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", uint16(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", uint32(42), &actual.i64, allTypes{i64: 42}},
		{"select $1::int8", uint64(42), &actual.i64, allTypes{i64: 42}},
	}

	for i, tt := range successfulEncodeTests {
		actual = zero

		err := conn.QueryRow(context.Background(), tt.sql, tt.queryArg).Scan(tt.scanArg)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArg -> %v)", i, err, tt.sql, tt.queryArg)
			continue
		}

		if actual != tt.expected {
			t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArg -> %v)", i, tt.expected, actual, tt.sql, tt.queryArg)
		}

		ensureConnValid(t, conn)
	}

	failedEncodeTests := []struct {
		sql      string
		queryArg any
	}{
		// Check any integer type where value is outside pg:int2 range cannot be encoded
		{"select $1::int2", int(32769)},
		{"select $1::int2", int32(32769)},
		{"select $1::int2", int32(32769)},
		{"select $1::int2", int64(32769)},
		{"select $1::int2", uint(32769)},
		{"select $1::int2", uint16(32769)},
		{"select $1::int2", uint32(32769)},
		{"select $1::int2", uint64(32769)},

		// Check any integer type where value is outside pg:int4 range cannot be encoded
		{"select $1::int4", int64(2147483649)},
		{"select $1::int4", uint32(2147483649)},
		{"select $1::int4", uint64(2147483649)},

		// Check any integer type where value is outside pg:int8 range cannot be encoded
		{"select $1::int8", uint64(9223372036854775809)},
	}

	for i, tt := range failedEncodeTests {
		err := conn.QueryRow(context.Background(), tt.sql, tt.queryArg).Scan(nil)
		if err == nil {
			t.Errorf("%d. Expected failure to encode, but unexpectedly succeeded: %v (sql -> %v, queryArg -> %v)", i, err, tt.sql, tt.queryArg)
		} else if !strings.Contains(err.Error(), "is greater than") {
			t.Errorf("%d. Expected failure to encode, but got: %v (sql -> %v, queryArg -> %v)", i, err, tt.sql, tt.queryArg)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowCoreIntegerDecoding(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	type allTypes struct {
		ui   uint
		ui8  uint8
		ui16 uint16
		ui32 uint32
		ui64 uint64
		i    int
		i8   int8
		i16  int16
		i32  int32
		i64  int64
	}

	var actual, zero allTypes

	successfulDecodeTests := []struct {
		sql      string
		scanArg  any
		expected allTypes
	}{
		// Check any integer type where value is within Go:int range can be decoded
		{"select 42::int2", &actual.i, allTypes{i: 42}},
		{"select 42::int4", &actual.i, allTypes{i: 42}},
		{"select 42::int8", &actual.i, allTypes{i: 42}},
		{"select -42::int2", &actual.i, allTypes{i: -42}},
		{"select -42::int4", &actual.i, allTypes{i: -42}},
		{"select -42::int8", &actual.i, allTypes{i: -42}},

		// Check any integer type where value is within Go:int8 range can be decoded
		{"select 42::int2", &actual.i8, allTypes{i8: 42}},
		{"select 42::int4", &actual.i8, allTypes{i8: 42}},
		{"select 42::int8", &actual.i8, allTypes{i8: 42}},
		{"select -42::int2", &actual.i8, allTypes{i8: -42}},
		{"select -42::int4", &actual.i8, allTypes{i8: -42}},
		{"select -42::int8", &actual.i8, allTypes{i8: -42}},

		// Check any integer type where value is within Go:int16 range can be decoded
		{"select 42::int2", &actual.i16, allTypes{i16: 42}},
		{"select 42::int4", &actual.i16, allTypes{i16: 42}},
		{"select 42::int8", &actual.i16, allTypes{i16: 42}},
		{"select -42::int2", &actual.i16, allTypes{i16: -42}},
		{"select -42::int4", &actual.i16, allTypes{i16: -42}},
		{"select -42::int8", &actual.i16, allTypes{i16: -42}},

		// Check any integer type where value is within Go:int32 range can be decoded
		{"select 42::int2", &actual.i32, allTypes{i32: 42}},
		{"select 42::int4", &actual.i32, allTypes{i32: 42}},
		{"select 42::int8", &actual.i32, allTypes{i32: 42}},
		{"select -42::int2", &actual.i32, allTypes{i32: -42}},
		{"select -42::int4", &actual.i32, allTypes{i32: -42}},
		{"select -42::int8", &actual.i32, allTypes{i32: -42}},

		// Check any integer type where value is within Go:int64 range can be decoded
		{"select 42::int2", &actual.i64, allTypes{i64: 42}},
		{"select 42::int4", &actual.i64, allTypes{i64: 42}},
		{"select 42::int8", &actual.i64, allTypes{i64: 42}},
		{"select -42::int2", &actual.i64, allTypes{i64: -42}},
		{"select -42::int4", &actual.i64, allTypes{i64: -42}},
		{"select -42::int8", &actual.i64, allTypes{i64: -42}},

		// Check any integer type where value is within Go:uint range can be decoded
		{"select 128::int2", &actual.ui, allTypes{ui: 128}},
		{"select 128::int4", &actual.ui, allTypes{ui: 128}},
		{"select 128::int8", &actual.ui, allTypes{ui: 128}},

		// Check any integer type where value is within Go:uint8 range can be decoded
		{"select 128::int2", &actual.ui8, allTypes{ui8: 128}},
		{"select 128::int4", &actual.ui8, allTypes{ui8: 128}},
		{"select 128::int8", &actual.ui8, allTypes{ui8: 128}},

		// Check any integer type where value is within Go:uint16 range can be decoded
		{"select 42::int2", &actual.ui16, allTypes{ui16: 42}},
		{"select 32768::int4", &actual.ui16, allTypes{ui16: 32768}},
		{"select 32768::int8", &actual.ui16, allTypes{ui16: 32768}},

		// Check any integer type where value is within Go:uint32 range can be decoded
		{"select 42::int2", &actual.ui32, allTypes{ui32: 42}},
		{"select 42::int4", &actual.ui32, allTypes{ui32: 42}},
		{"select 2147483648::int8", &actual.ui32, allTypes{ui32: 2147483648}},

		// Check any integer type where value is within Go:uint64 range can be decoded
		{"select 42::int2", &actual.ui64, allTypes{ui64: 42}},
		{"select 42::int4", &actual.ui64, allTypes{ui64: 42}},
		{"select 42::int8", &actual.ui64, allTypes{ui64: 42}},
	}

	for i, tt := range successfulDecodeTests {
		actual = zero

		err := conn.QueryRow(context.Background(), tt.sql).Scan(tt.scanArg)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v)", i, err, tt.sql)
			continue
		}

		if actual != tt.expected {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.expected, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}

	failedDecodeTests := []struct {
		sql     string
		scanArg any
	}{
		// Check any integer type where value is outside Go:int8 range cannot be decoded
		{"select 128::int2", &actual.i8},
		{"select 128::int4", &actual.i8},
		{"select 128::int8", &actual.i8},
		{"select -129::int2", &actual.i8},
		{"select -129::int4", &actual.i8},
		{"select -129::int8", &actual.i8},

		// Check any integer type where value is outside Go:int16 range cannot be decoded
		{"select 32768::int4", &actual.i16},
		{"select 32768::int8", &actual.i16},
		{"select -32769::int4", &actual.i16},
		{"select -32769::int8", &actual.i16},

		// Check any integer type where value is outside Go:int32 range cannot be decoded
		{"select 2147483648::int8", &actual.i32},
		{"select -2147483649::int8", &actual.i32},

		// Check any integer type where value is outside Go:uint range cannot be decoded
		{"select -1::int2", &actual.ui},
		{"select -1::int4", &actual.ui},
		{"select -1::int8", &actual.ui},

		// Check any integer type where value is outside Go:uint8 range cannot be decoded
		{"select 256::int2", &actual.ui8},
		{"select 256::int4", &actual.ui8},
		{"select 256::int8", &actual.ui8},
		{"select -1::int2", &actual.ui8},
		{"select -1::int4", &actual.ui8},
		{"select -1::int8", &actual.ui8},

		// Check any integer type where value is outside Go:uint16 cannot be decoded
		{"select 65536::int4", &actual.ui16},
		{"select 65536::int8", &actual.ui16},
		{"select -1::int2", &actual.ui16},
		{"select -1::int4", &actual.ui16},
		{"select -1::int8", &actual.ui16},

		// Check any integer type where value is outside Go:uint32 range cannot be decoded
		{"select 4294967296::int8", &actual.ui32},
		{"select -1::int2", &actual.ui32},
		{"select -1::int4", &actual.ui32},
		{"select -1::int8", &actual.ui32},

		// Check any integer type where value is outside Go:uint64 range cannot be decoded
		{"select -1::int2", &actual.ui64},
		{"select -1::int4", &actual.ui64},
		{"select -1::int8", &actual.ui64},
	}

	for i, tt := range failedDecodeTests {
		err := conn.QueryRow(context.Background(), tt.sql).Scan(tt.scanArg)
		if err == nil {
			t.Errorf("%d. Expected failure to decode, but unexpectedly succeeded: %v (sql -> %v)", i, err, tt.sql)
		} else if !strings.Contains(err.Error(), "can't scan") {
			t.Errorf("%d. Expected failure to decode, but got: %v (sql -> %v)", i, err, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowCoreByteSlice(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tests := []struct {
		sql      string
		queryArg any
		expected []byte
	}{
		{"select $1::text", "Jack", []byte("Jack")},
		{"select $1::text", []byte("Jack"), []byte("Jack")},
		{"select $1::varchar", []byte("Jack"), []byte("Jack")},
		{"select $1::bytea", []byte{0, 15, 255, 17}, []byte{0, 15, 255, 17}},
	}

	for i, tt := range tests {
		var actual []byte

		err := conn.QueryRow(context.Background(), tt.sql, tt.queryArg).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v)", i, err, tt.sql)
		}

		if !bytes.Equal(actual, tt.expected) {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.expected, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowErrors(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		t.Skip("Skipping due to known server missing point type")
	}

	type allTypes struct {
		i16 int16
		s   string
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []any
		scanArgs  []any
		err       string
	}{
		{"select $1::badtype", []any{"Jack"}, []any{&actual.i16}, `type "badtype" does not exist`},
		{"SYNTAX ERROR", []any{}, []any{&actual.i16}, "SQLSTATE 42601"},
		{"select $1::text", []any{"Jack"}, []any{&actual.i16}, "cannot scan text (OID 25) in text format into *int16"},
		{"select $1::point", []any{int(705)}, []any{&actual.s}, "unable to encode 705 into binary format for point (OID 600)"},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(context.Background(), tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err == nil {
			t.Errorf("%d. Unexpected success (sql -> %v, queryArgs -> %v)", i, tt.sql, tt.queryArgs)
		}
		if err != nil && !strings.Contains(err.Error(), tt.err) {
			t.Errorf("%d. Expected error to contain %s, but got %v (sql -> %v, queryArgs -> %v)", i, tt.err, err, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowNoResults(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var n int32
	err := conn.QueryRow(context.Background(), "select 1 where 1=0").Scan(&n)
	if err != pgx.ErrNoRows {
		t.Errorf("Expected pgx.ErrNoRows, got %v", err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowEmptyQuery(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var n int32
	err := conn.QueryRow(ctx, "").Scan(&n)
	require.Error(t, err)
	require.False(t, pgconn.Timeout(err))

	ensureConnValid(t, conn)
}

func TestReadingValueAfterEmptyArray(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var a []string
	var b int32
	err := conn.QueryRow(context.Background(), "select '{}'::text[], 42::integer").Scan(&a, &b)
	if err != nil {
		t.Fatalf("conn.QueryRow failed: %v", err)
	}

	if len(a) != 0 {
		t.Errorf("Expected 'a' to have length 0, but it was: %d", len(a))
	}

	if b != 42 {
		t.Errorf("Expected 'b' to 42, but it was: %d", b)
	}
}

func TestReadingNullByteArray(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var a []byte
	err := conn.QueryRow(context.Background(), "select null::text").Scan(&a)
	if err != nil {
		t.Fatalf("conn.QueryRow failed: %v", err)
	}

	if a != nil {
		t.Errorf("Expected 'a' to be nil, but it was: %v", a)
	}
}

func TestReadingNullByteArrays(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "select null::text union all select null::text")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
		var a []byte
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		if a != nil {
			t.Errorf("Expected 'a' to be nil, but it was: %v", a)
		}
	}
	if count != 2 {
		t.Errorf("Expected to read 2 rows, read: %d", count)
	}
}

func TestQueryNullSliceIsSet(t *testing.T) {
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	a := []int32{1, 2, 3}
	err := conn.QueryRow(context.Background(), "select null::int[]").Scan(&a)
	if err != nil {
		t.Fatalf("conn.QueryRow failed: %v", err)
	}

	if a != nil {
		t.Errorf("Expected 'a' to be nil, but it was: %v", a)
	}
}

func TestConnQueryDatabaseSQLScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var num sql.NullFloat64

	err := conn.QueryRow(context.Background(), "select '1234.567'::float8").Scan(&num)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	require.True(t, num.Valid)
	require.Equal(t, 1234.567, num.Float64)

	ensureConnValid(t, conn)
}

func TestConnQueryDatabaseSQLDriverValuer(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	expected := sql.NullFloat64{Float64: 1234.567, Valid: true}
	var actual sql.NullFloat64

	err := conn.QueryRow(context.Background(), "select $1::float8", &expected).Scan(&actual)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/339
func TestConnQueryDatabaseSQLDriverValuerWithAutoGeneratedPointerReceiver(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table t(n numeric)")

	var d *sql.NullInt64
	commandTag, err := conn.Exec(context.Background(), `insert into t(n) values($1)`, d)
	if err != nil {
		t.Fatal(err)
	}
	if commandTag.String() != "INSERT 0 1" {
		t.Fatalf("want %s, got %s", "INSERT 0 1", commandTag)
	}

	ensureConnValid(t, conn)
}

type nilPointerAsEmptyJSONObject struct {
	ID   string
	Name string
}

func (v *nilPointerAsEmptyJSONObject) Value() (driver.Value, error) {
	if v == nil {
		return "{}", nil
	}

	return json.Marshal(v)
}

// https://github.com/jackc/pgx/issues/1566
func TestConnQueryDatabaseSQLDriverValuerCalledOnNilPointerImplementers(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table t(v json not null)")

	var v *nilPointerAsEmptyJSONObject
	commandTag, err := conn.Exec(context.Background(), `insert into t(v) values($1)`, v)
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", commandTag.String())

	var s string
	err = conn.QueryRow(context.Background(), "select v from t").Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "{}", s)

	_, err = conn.Exec(context.Background(), `delete from t`)
	require.NoError(t, err)

	v = &nilPointerAsEmptyJSONObject{ID: "1", Name: "foo"}
	commandTag, err = conn.Exec(context.Background(), `insert into t(v) values($1)`, v)
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", commandTag.String())

	var v2 *nilPointerAsEmptyJSONObject
	err = conn.QueryRow(context.Background(), "select v from t").Scan(&v2)
	require.NoError(t, err)
	require.Equal(t, v, v2)

	ensureConnValid(t, conn)
}

type nilSliceAsEmptySlice []byte

func (j nilSliceAsEmptySlice) Value() (driver.Value, error) {
	if len(j) == 0 {
		return []byte("[]"), nil
	}

	return []byte(j), nil
}

func (j *nilSliceAsEmptySlice) UnmarshalJSON(data []byte) error {
	*j = bytes.Clone(data)
	return nil
}

// https://github.com/jackc/pgx/issues/1860
func TestConnQueryDatabaseSQLDriverValuerCalledOnNilSliceImplementers(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table t(v json not null)")

	var v nilSliceAsEmptySlice
	commandTag, err := conn.Exec(context.Background(), `insert into t(v) values($1)`, v)
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", commandTag.String())

	var s string
	err = conn.QueryRow(context.Background(), "select v from t").Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "[]", s)

	_, err = conn.Exec(context.Background(), `delete from t`)
	require.NoError(t, err)

	v = nilSliceAsEmptySlice(`{"name": "foo"}`)
	commandTag, err = conn.Exec(context.Background(), `insert into t(v) values($1)`, v)
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", commandTag.String())

	var v2 nilSliceAsEmptySlice
	err = conn.QueryRow(context.Background(), "select v from t").Scan(&v2)
	require.NoError(t, err)
	require.Equal(t, v, v2)

	ensureConnValid(t, conn)
}

type nilMapAsEmptyObject map[string]any

func (j nilMapAsEmptyObject) Value() (driver.Value, error) {
	if j == nil {
		return []byte("{}"), nil
	}

	return json.Marshal(j)
}

func (j *nilMapAsEmptyObject) UnmarshalJSON(data []byte) error {
	var m map[string]any
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	*j = m

	return nil
}

// https://github.com/jackc/pgx/pull/2019#discussion_r1605806751
func TestConnQueryDatabaseSQLDriverValuerCalledOnNilMapImplementers(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table t(v json not null)")

	var v nilMapAsEmptyObject
	commandTag, err := conn.Exec(context.Background(), `insert into t(v) values($1)`, v)
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", commandTag.String())

	var s string
	err = conn.QueryRow(context.Background(), "select v from t").Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "{}", s)

	_, err = conn.Exec(context.Background(), `delete from t`)
	require.NoError(t, err)

	v = nilMapAsEmptyObject{"name": "foo"}
	commandTag, err = conn.Exec(context.Background(), `insert into t(v) values($1)`, v)
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", commandTag.String())

	var v2 nilMapAsEmptyObject
	err = conn.QueryRow(context.Background(), "select v from t").Scan(&v2)
	require.NoError(t, err)
	require.Equal(t, v, v2)

	ensureConnValid(t, conn)
}

func TestConnQueryDatabaseSQLDriverScannerWithBinaryPgTypeThatAcceptsSameType(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var actual sql.NullString
	err := conn.QueryRow(context.Background(), "select '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid").Scan(&actual)
	require.NoError(t, err)

	require.True(t, actual.Valid)
	require.Equal(t, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", actual.String)

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/1273#issuecomment-1221672175
func TestConnQueryDatabaseSQLDriverValuerTextWhenBinaryIsPreferred(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	arg := sql.NullString{String: "1.234", Valid: true}
	var result pgtype.Numeric
	err := conn.QueryRow(context.Background(), "select $1::numeric", arg).Scan(&result)
	require.NoError(t, err)

	require.True(t, result.Valid)
	f64, err := result.Float64Value()
	require.NoError(t, err)
	require.Equal(t, pgtype.Float8{Float64: 1.234, Valid: true}, f64)

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/1426
func TestConnQueryDatabaseSQLNullFloat64NegativeZeroPointZero(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	tests := []float64{
		-0.01,
		-0.001,
		-0.0001,
	}

	for _, val := range tests {
		var result sql.NullFloat64
		err := conn.QueryRow(context.Background(), "select $1::numeric", val).Scan(&result)
		require.NoError(t, err)
		require.Equal(t, sql.NullFloat64{Float64: val, Valid: true}, result)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryDatabaseSQLNullX(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	type row struct {
		boolValid    sql.NullBool
		boolNull     sql.NullBool
		int64Valid   sql.NullInt64
		int64Null    sql.NullInt64
		float64Valid sql.NullFloat64
		float64Null  sql.NullFloat64
		stringValid  sql.NullString
		stringNull   sql.NullString
	}

	expected := row{
		boolValid:    sql.NullBool{Bool: true, Valid: true},
		int64Valid:   sql.NullInt64{Int64: 123, Valid: true},
		float64Valid: sql.NullFloat64{Float64: 3.14, Valid: true},
		stringValid:  sql.NullString{String: "pgx", Valid: true},
	}

	var actual row

	err := conn.QueryRow(
		context.Background(),
		"select $1::bool, $2::bool, $3::int8, $4::int8, $5::float8, $6::float8, $7::text, $8::text",
		expected.boolValid,
		expected.boolNull,
		expected.int64Valid,
		expected.int64Null,
		expected.float64Valid,
		expected.float64Null,
		expected.stringValid,
		expected.stringNull,
	).Scan(
		&actual.boolValid,
		&actual.boolNull,
		&actual.int64Valid,
		&actual.int64Null,
		&actual.float64Valid,
		&actual.float64Null,
		&actual.stringValid,
		&actual.stringNull,
	)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if expected != actual {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}

	ensureConnValid(t, conn)
}

func TestQueryContextSuccess(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	rows, err := conn.Query(ctx, "select 42::integer")
	if err != nil {
		t.Fatal(err)
	}

	var result, rowCount int
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			t.Fatal(err)
		}
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}

	if rowCount != 1 {
		t.Fatalf("Expected 1 row, got %d", rowCount)
	}
	if result != 42 {
		t.Fatalf("Expected result 42, got %d", result)
	}

	ensureConnValid(t, conn)
}

func TestQueryContextErrorWhileReceivingRows(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server uses numeric instead of int")

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	rows, err := conn.Query(ctx, "select 10/(10-n) from generate_series(1, 100) n")
	if err != nil {
		t.Fatal(err)
	}

	var result, rowCount int
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			t.Fatal(err)
		}
		rowCount++
	}

	if rows.Err() == nil || rows.Err().Error() != "ERROR: division by zero (SQLSTATE 22012)" {
		t.Fatalf("Expected division by zero error, but got %v", rows.Err())
	}

	if rowCount != 9 {
		t.Fatalf("Expected 9 rows, got %d", rowCount)
	}
	if result != 10 {
		t.Fatalf("Expected result 10, got %d", result)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowContextSuccess(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var result int
	err := conn.QueryRow(ctx, "select 42::integer").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("Expected result 42, got %d", result)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowContextErrorWhileReceivingRow(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var result int
	err := conn.QueryRow(ctx, "select 10/0").Scan(&result)
	if err == nil || err.Error() != "ERROR: division by zero (SQLSTATE 22012)" {
		t.Fatalf("Expected division by zero error, but got %v", err)
	}

	ensureConnValid(t, conn)
}

func TestQueryCloseBefore(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	closeConn(t, conn)

	_, err := conn.Query(context.Background(), "select 1")
	require.Error(t, err)
	assert.True(t, pgconn.SafeToRetry(err))
}

func TestScanRow(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	resultReader := conn.PgConn().ExecParams(context.Background(), "select generate_series(1,$1)", [][]byte{[]byte("10")}, nil, nil, nil)

	var sum, rowCount int32

	for resultReader.NextRow() {
		var n int32
		err := pgx.ScanRow(conn.TypeMap(), resultReader.FieldDescriptions(), resultReader.Values(), &n)
		assert.NoError(t, err)
		sum += n
		rowCount++
	}

	_, err := resultReader.Close()

	require.NoError(t, err)
	assert.EqualValues(t, 10, rowCount)
	assert.EqualValues(t, 55, sum)
}

func TestConnSimpleProtocol(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	// Test all supported low-level types

	{
		expected := int64(42)
		var actual int64
		err := conn.QueryRow(
			context.Background(),
			"select $1::int8",
			pgx.QueryExecModeSimpleProtocol,
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if expected != actual {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	{
		expected := float64(1.23)
		var actual float64
		err := conn.QueryRow(
			context.Background(),
			"select $1::float8",
			pgx.QueryExecModeSimpleProtocol,
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if expected != actual {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	{
		expected := true
		var actual bool
		err := conn.QueryRow(
			context.Background(),
			"select $1::boolean",
			pgx.QueryExecModeSimpleProtocol,
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if expected != actual {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	{
		expected := []byte{0, 1, 20, 35, 64, 80, 120, 3, 255, 240, 128, 95}
		var actual []byte
		err := conn.QueryRow(
			context.Background(),
			"select $1::bytea",
			pgx.QueryExecModeSimpleProtocol,
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(actual, expected) {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	{
		expected := "test"
		var actual string
		err := conn.QueryRow(
			context.Background(),
			"select $1::text",
			pgx.QueryExecModeSimpleProtocol,
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if expected != actual {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	{
		tests := []struct {
			expected []string
		}{
			{[]string(nil)},
			{[]string{}},
			{[]string{"test", "foo", "bar"}},
			{[]string{`foo'bar"\baz;quz`, `foo'bar"\baz;quz`}},
		}
		for i, tt := range tests {
			var actual []string
			err := conn.QueryRow(
				context.Background(),
				"select $1::text[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []int16
		}{
			{[]int16(nil)},
			{[]int16{}},
			{[]int16{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []int16
			err := conn.QueryRow(
				context.Background(),
				"select $1::smallint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []int32
		}{
			{[]int32(nil)},
			{[]int32{}},
			{[]int32{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []int32
			err := conn.QueryRow(
				context.Background(),
				"select $1::int[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []int64
		}{
			{[]int64(nil)},
			{[]int64{}},
			{[]int64{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []int64
			err := conn.QueryRow(
				context.Background(),
				"select $1::bigint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []int
		}{
			{[]int(nil)},
			{[]int{}},
			{[]int{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []int
			err := conn.QueryRow(
				context.Background(),
				"select $1::bigint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []uint16
		}{
			{[]uint16(nil)},
			{[]uint16{}},
			{[]uint16{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []uint16
			err := conn.QueryRow(
				context.Background(),
				"select $1::smallint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []uint32
		}{
			{[]uint32(nil)},
			{[]uint32{}},
			{[]uint32{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []uint32
			err := conn.QueryRow(
				context.Background(),
				"select $1::bigint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []uint64
		}{
			{[]uint64(nil)},
			{[]uint64{}},
			{[]uint64{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []uint64
			err := conn.QueryRow(
				context.Background(),
				"select $1::bigint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []uint
		}{
			{[]uint(nil)},
			{[]uint{}},
			{[]uint{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []uint
			err := conn.QueryRow(
				context.Background(),
				"select $1::bigint[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []float32
		}{
			{[]float32(nil)},
			{[]float32{}},
			{[]float32{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []float32
			err := conn.QueryRow(
				context.Background(),
				"select $1::float4[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	{
		tests := []struct {
			expected []float64
		}{
			{[]float64(nil)},
			{[]float64{}},
			{[]float64{1, 2, 3}},
		}
		for i, tt := range tests {
			var actual []float64
			err := conn.QueryRow(
				context.Background(),
				"select $1::float8[]",
				pgx.QueryExecModeSimpleProtocol,
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	}

	// Test high-level type

	{
		if conn.PgConn().ParameterStatus("crdb_version") == "" {
			// CockroachDB doesn't support circle type.
			expected := pgtype.Circle{P: pgtype.Vec2{X: 1, Y: 2}, R: 1.5, Valid: true}
			actual := expected
			err := conn.QueryRow(
				context.Background(),
				"select $1::circle",
				pgx.QueryExecModeSimpleProtocol,
				&expected,
			).Scan(&actual)
			if err != nil {
				t.Error(err)
			}
			if expected != actual {
				t.Errorf("expected %v got %v", expected, actual)
			}
		}
	}

	// Test multiple args in single query

	{
		expectedInt64 := int64(234423)
		expectedFloat64 := float64(-0.2312)
		expectedBool := true
		expectedBytes := []byte{255, 0, 23, 16, 87, 45, 9, 23, 45, 223}
		expectedString := "test"
		var actualInt64 int64
		var actualFloat64 float64
		var actualBool bool
		var actualBytes []byte
		var actualString string
		err := conn.QueryRow(
			context.Background(),
			"select $1::int8, $2::float8, $3::boolean, $4::bytea, $5::text",
			pgx.QueryExecModeSimpleProtocol,
			expectedInt64, expectedFloat64, expectedBool, expectedBytes, expectedString,
		).Scan(&actualInt64, &actualFloat64, &actualBool, &actualBytes, &actualString)
		if err != nil {
			t.Error(err)
		}
		if expectedInt64 != actualInt64 {
			t.Errorf("expected %v got %v", expectedInt64, actualInt64)
		}
		if expectedFloat64 != actualFloat64 {
			t.Errorf("expected %v got %v", expectedFloat64, actualFloat64)
		}
		if expectedBool != actualBool {
			t.Errorf("expected %v got %v", expectedBool, actualBool)
		}
		if !bytes.Equal(expectedBytes, actualBytes) {
			t.Errorf("expected %v got %v", expectedBytes, actualBytes)
		}
		if expectedString != actualString {
			t.Errorf("expected %v got %v", expectedString, actualString)
		}
	}

	// Test dangerous cases

	{
		expected := "foo';drop table users;"
		var actual string
		err := conn.QueryRow(
			context.Background(),
			"select $1",
			pgx.QueryExecModeSimpleProtocol,
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if expected != actual {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	ensureConnValid(t, conn)
}

func TestConnSimpleProtocolRefusesNonUTF8ClientEncoding(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server does not support changing client_encoding (https://www.cockroachlabs.com/docs/stable/set-vars.html)")

	mustExec(t, conn, "set client_encoding to 'SQL_ASCII'")

	var expected string
	err := conn.QueryRow(
		context.Background(),
		"select $1",
		pgx.QueryExecModeSimpleProtocol,
		"test",
	).Scan(&expected)
	if err == nil {
		t.Error("expected error when client_encoding not UTF8, but no error occurred")
	}

	ensureConnValid(t, conn)
}

func TestConnSimpleProtocolRefusesNonStandardConformingStrings(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server does not support standard_conforming_strings = off (https://github.com/cockroachdb/cockroach/issues/36215)")

	mustExec(t, conn, "set standard_conforming_strings to off")

	var expected string
	err := conn.QueryRow(
		context.Background(),
		"select $1",
		pgx.QueryExecModeSimpleProtocol,
		`\'; drop table users; --`,
	).Scan(&expected)
	if err == nil {
		t.Error("expected error when standard_conforming_strings is off, but no error occurred")
	}

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/895
func TestQueryErrorWithDisabledStatementCache(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	config.StatementCacheCapacity = 0
	config.DescriptionCacheCapacity = 0

	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	_, err := conn.Exec(context.Background(), "create temporary table t_unq(id text primary key);")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "insert into t_unq (id) values ($1)", "abc")
	require.NoError(t, err)

	rows, err := conn.Query(context.Background(), "insert into t_unq (id) values ($1)", "abc")
	require.NoError(t, err)
	rows.Close()
	err = rows.Err()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		assert.Equal(t, "23505", pgErr.Code)
	} else {
		t.Errorf("err is not a *pgconn.PgError: %T", err)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryQueryExecModeCacheDescribeSafeEvenWhenTypesChange(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	pgxtest.SkipCockroachDB(t, conn, "Server does not support alter column type from int to float4")

	_, err := conn.Exec(ctx, `create temporary table to_change (
	name text primary key,
	age int
);

insert into to_change (name, age) values ('John', 42);`)
	require.NoError(t, err)

	var name string
	var ageInt32 int32
	err = conn.QueryRow(ctx, "select * from to_change where age = $1", pgx.QueryExecModeCacheDescribe, int32(42)).Scan(&name, &ageInt32)
	require.NoError(t, err)
	require.Equal(t, "John", name)
	require.Equal(t, int32(42), ageInt32)

	_, err = conn.Exec(ctx, `alter table to_change alter column age type float4;`)
	require.NoError(t, err)

	err = conn.QueryRow(ctx, "select * from to_change where age = $1", pgx.QueryExecModeCacheDescribe, int32(42)).Scan(&name, &ageInt32)
	require.NoError(t, err)
	require.Equal(t, "John", name)
	require.Equal(t, int32(42), ageInt32)

	var ageFloat32 float32
	err = conn.QueryRow(ctx, "select * from to_change where age = $1", pgx.QueryExecModeCacheDescribe, int32(42)).Scan(&name, &ageFloat32)
	require.NoError(t, err)
	require.Equal(t, "John", name)
	require.Equal(t, float32(42), ageFloat32)

	_, err = conn.Exec(ctx, `alter table to_change drop column name;`)
	require.NoError(t, err)

	// Number of result columns has changed, so just like with a prepared statement, this will fail the first time.
	err = conn.QueryRow(ctx, "select * from to_change where age = $1", pgx.QueryExecModeCacheDescribe, int32(42)).Scan(&ageFloat32)
	require.EqualError(t, err, "ERROR: bind message has 2 result formats but query has 1 columns (SQLSTATE 08P01)")

	// But it will work the second time after the cache is invalidated.
	err = conn.QueryRow(ctx, "select * from to_change where age = $1", pgx.QueryExecModeCacheDescribe, int32(42)).Scan(&ageFloat32)
	require.NoError(t, err)
	require.Equal(t, float32(42), ageFloat32)

	_, err = conn.Exec(ctx, `alter table to_change alter column age type numeric;`)
	require.NoError(t, err)

	err = conn.QueryRow(ctx, "select * from to_change where age = $1", pgx.QueryExecModeCacheDescribe, int32(42)).Scan(&ageFloat32)
	require.NoError(t, err)
	require.Equal(t, float32(42), ageFloat32)
}

func TestQueryWithQueryRewriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		qr := testQueryRewriter{sql: "select $1::int", args: []any{42}}
		rows, err := conn.Query(ctx, "should be replaced", &qr)
		require.NoError(t, err)

		var n int32
		var rowCount int
		for rows.Next() {
			rowCount++
			err = rows.Scan(&n)
			require.NoError(t, err)
		}

		require.NoError(t, rows.Err())
	})
}

// This example uses Query without using any helpers to read the results. Normally CollectRows, ForEachRow, or another
// helper function should be used.
func ExampleConn_Query() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		// Skip test / example when running on CockroachDB. Since an example can't be skipped fake success instead.
		fmt.Println(`Cheeseburger: $10
Fries: $5
Soft Drink: $3`)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table products (
	id int primary key generated by default as identity,
	name varchar(100) not null,
	price int not null
);

insert into products (name, price) values
	('Cheeseburger', 10),
	('Double Cheeseburger', 14),
	('Fries', 5),
	('Soft Drink', 3);
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	rows, err := conn.Query(ctx, "select name, price from products where price < $1 order by price desc", 12)

	// It is unnecessary to check err. If an error occurred it will be returned by rows.Err() later. But in rare
	// cases it may be useful to detect the error as early as possible.
	if err != nil {
		fmt.Printf("Query error: %v", err)
		return
	}

	// Ensure rows is closed. It is safe to close rows multiple times.
	defer rows.Close()

	// Iterate through the result set
	for rows.Next() {
		var name string
		var price int32

		err = rows.Scan(&name, &price)
		if err != nil {
			fmt.Printf("Scan error: %v", err)
			return
		}

		fmt.Printf("%s: $%d\n", name, price)
	}

	// rows is closed automatically when rows.Next() returns false so it is not necessary to manually close rows.

	// The first error encountered by the original Query call, rows.Next or rows.Scan will be returned here.
	if rows.Err() != nil {
		fmt.Printf("rows error: %v", rows.Err())
		return
	}

	// Output:
	// Cheeseburger: $10
	// Fries: $5
	// Soft Drink: $3
}
