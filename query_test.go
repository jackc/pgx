package pgx_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/gofrs/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgconn/stmtcache"
	"github.com/jackc/pgtype"
	gofrs "github.com/jackc/pgtype/ext/gofrs-uuid"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
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
		t.Fatalf("conn.Query failed: %v", err)
	}

	assert.Equal(t, "SELECT 10", string(rows.CommandTag()))

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
	assert.Equal(t, []byte("msg"), rows.FieldDescriptions()[0].Name)
}

func TestConnQueryWithoutResultSetCommandTag(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	rows, err := conn.Query(context.Background(), "create temporary table t (id serial);")
	assert.NoError(t, err)
	rows.Close()
	assert.NoError(t, rows.Err())
	assert.Equal(t, "CREATE TABLE", string(rows.CommandTag()))
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
		destPtrs := make([]interface{}, columnCount)
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
		t.Fatalf("conn.Query failed: %v", err)
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
		t.Fatalf("conn.Query failed: %v", err)
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

func TestConnQueryValuesWithUnknownOID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			var d pgtype.Unknown
			var e int32

			err = rows.Scan(&a, &b, &c, &d, &e)
			require.NoError(t, err)
			require.Equal(t, "foo", a)
			require.Equal(t, "bar", b)
			require.Equal(t, rowCount, c)
			require.Equal(t, pgtype.Null, d.Status)
			require.Equal(t, rowCount, e)
		}
	}

	require.NoError(t, rows.Err())
	require.Equal(t, int32(10), rowCount)
}

// https://github.com/jackc/pgx/issues/386
func TestConnQueryValuesWithMultipleComplexColumnsOfSameType(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	expected0 := &pgtype.Int8Array{
		Elements: []pgtype.Int8{
			{Int: 1, Status: pgtype.Present},
			{Int: 2, Status: pgtype.Present},
			{Int: 3, Status: pgtype.Present},
		},
		Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}},
		Status:     pgtype.Present,
	}

	expected1 := &pgtype.Int8Array{
		Elements: []pgtype.Int8{
			{Int: 4, Status: pgtype.Present},
			{Int: 5, Status: pgtype.Present},
			{Int: 6, Status: pgtype.Present},
		},
		Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}},
		Status:     pgtype.Present,
	}

	var rowCount int32

	rows, err := conn.Query(context.Background(), "select '{1,2,3}'::bigint[], '{4,5,6}'::bigint[] from generate_series(1,$1) n", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		rowCount++

		values, err := rows.Values()
		if err != nil {
			t.Fatalf("rows.Values failed: %v", err)
		}
		if len(values) != 2 {
			t.Errorf("Expected rows.Values to return 2 values, but it returned %d", len(values))
		}
		if !reflect.DeepEqual(values[0], *expected0) {
			t.Errorf(`Expected values[0] to be %v, but it was %v`, *expected0, values[0])
		}
		if !reflect.DeepEqual(values[1], *expected1) {
			t.Errorf(`Expected values[1] to be %v, but it was %v`, *expected1, values[1])
		}
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
}

// https://github.com/jackc/pgx/issues/228
func TestRowsScanDoesNotAllowScanningBinaryFormatValuesIntoString(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var s string

	err := conn.QueryRow(context.Background(), "select 1").Scan(&s)
	if err == nil || !(strings.Contains(err.Error(), "cannot decode binary value into string") || strings.Contains(err.Error(), "cannot assign")) {
		t.Fatalf("Expected Scan to fail to encode binary value into string but: %v", err)
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
		pgx.QuerySimpleProtocol(true),
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
	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
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

	if rows.Err().Error() != "can't scan into dest[0]: Can't convert OID 23 to time.Time" && !strings.Contains(rows.Err().Error(), "cannot assign") {
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

	skipCockroachDB(t, conn, "Server does not support deferred constraint (https://github.com/cockroachdb/cockroach/issues/31632)")

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

	skipCockroachDB(t, conn, "Server uses numeric instead of int")

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
				t.Fatalf("Expected pgx.PgError, got %v", rows.Err())
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
	if conn.PgConn().ParameterStatus("crdb_version") != "" {
		if !strings.Contains(rows.Err().Error(), "SQLSTATE 08P01") {
			// CockroachDB returns protocol_violation instead of invalid_text_representation
			t.Error("Expected rows.Err() to return different error:", rows.Err())
		}
	} else {
		if !strings.Contains(rows.Err().Error(), "SQLSTATE 22P02") {
			t.Error("Expected rows.Err() to return different error:", rows.Err())
		}
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
		queryArgs []interface{}
		scanArgs  []interface{}
		expected  allTypes
	}{
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.s}, allTypes{s: "Jack"}},
		{"select $1::float4", []interface{}{float32(1.23)}, []interface{}{&actual.f32}, allTypes{f32: 1.23}},
		{"select $1::float8", []interface{}{float64(1.23)}, []interface{}{&actual.f64}, allTypes{f64: 1.23}},
		{"select $1::bool", []interface{}{true}, []interface{}{&actual.b}, allTypes{b: true}},
		{"select $1::timestamptz", []interface{}{time.Unix(123, 5000)}, []interface{}{&actual.t}, allTypes{t: time.Unix(123, 5000)}},
		{"select $1::timestamp", []interface{}{time.Date(2010, 1, 2, 3, 4, 5, 0, time.UTC)}, []interface{}{&actual.t}, allTypes{t: time.Date(2010, 1, 2, 3, 4, 5, 0, time.UTC)}},
		{"select $1::date", []interface{}{time.Date(1987, 1, 2, 0, 0, 0, 0, time.UTC)}, []interface{}{&actual.t}, allTypes{t: time.Date(1987, 1, 2, 0, 0, 0, 0, time.UTC)}},
		{"select $1::oid", []interface{}{uint32(42)}, []interface{}{&actual.oid}, allTypes{oid: 42}},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(context.Background(), tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, tt.sql, tt.queryArgs)
		}

		if actual != tt.expected {
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

	successfulEncodeTests := []struct {
		sql      string
		queryArg interface{}
		scanArg  interface{}
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
		queryArg interface{}
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
		scanArg  interface{}
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
		sql         string
		scanArg     interface{}
		expectedErr string
	}{
		// Check any integer type where value is outside Go:int8 range cannot be decoded
		{"select 128::int2", &actual.i8, "is greater than"},
		{"select 128::int4", &actual.i8, "is greater than"},
		{"select 128::int8", &actual.i8, "is greater than"},
		{"select -129::int2", &actual.i8, "is less than"},
		{"select -129::int4", &actual.i8, "is less than"},
		{"select -129::int8", &actual.i8, "is less than"},

		// Check any integer type where value is outside Go:int16 range cannot be decoded
		{"select 32768::int4", &actual.i16, "is greater than"},
		{"select 32768::int8", &actual.i16, "is greater than"},
		{"select -32769::int4", &actual.i16, "is less than"},
		{"select -32769::int8", &actual.i16, "is less than"},

		// Check any integer type where value is outside Go:int32 range cannot be decoded
		{"select 2147483648::int8", &actual.i32, "is greater than"},
		{"select -2147483649::int8", &actual.i32, "is less than"},

		// Check any integer type where value is outside Go:uint range cannot be decoded
		{"select -1::int2", &actual.ui, "is less than"},
		{"select -1::int4", &actual.ui, "is less than"},
		{"select -1::int8", &actual.ui, "is less than"},

		// Check any integer type where value is outside Go:uint8 range cannot be decoded
		{"select 256::int2", &actual.ui8, "is greater than"},
		{"select 256::int4", &actual.ui8, "is greater than"},
		{"select 256::int8", &actual.ui8, "is greater than"},
		{"select -1::int2", &actual.ui8, "is less than"},
		{"select -1::int4", &actual.ui8, "is less than"},
		{"select -1::int8", &actual.ui8, "is less than"},

		// Check any integer type where value is outside Go:uint16 cannot be decoded
		{"select 65536::int4", &actual.ui16, "is greater than"},
		{"select 65536::int8", &actual.ui16, "is greater than"},
		{"select -1::int2", &actual.ui16, "is less than"},
		{"select -1::int4", &actual.ui16, "is less than"},
		{"select -1::int8", &actual.ui16, "is less than"},

		// Check any integer type where value is outside Go:uint32 range cannot be decoded
		{"select 4294967296::int8", &actual.ui32, "is greater than"},
		{"select -1::int2", &actual.ui32, "is less than"},
		{"select -1::int4", &actual.ui32, "is less than"},
		{"select -1::int8", &actual.ui32, "is less than"},

		// Check any integer type where value is outside Go:uint64 range cannot be decoded
		{"select -1::int2", &actual.ui64, "is less than"},
		{"select -1::int4", &actual.ui64, "is less than"},
		{"select -1::int8", &actual.ui64, "is less than"},
	}

	for i, tt := range failedDecodeTests {
		err := conn.QueryRow(context.Background(), tt.sql).Scan(tt.scanArg)
		if err == nil {
			t.Errorf("%d. Expected failure to decode, but unexpectedly succeeded: %v (sql -> %v)", i, err, tt.sql)
		} else if !strings.Contains(err.Error(), tt.expectedErr) {
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
		queryArg interface{}
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

	type allTypes struct {
		i16 int16
		i   int
		s   string
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		// {"select $1::badtype", []interface{}{"Jack"}, []interface{}{&actual.i16}, `type "badtype" does not exist`},
		// {"SYNTAX ERROR", []interface{}{}, []interface{}{&actual.i16}, "SQLSTATE 42601"},
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.i16}, "unable to assign"},
		// {"select $1::point", []interface{}{int(705)}, []interface{}{&actual.s}, "cannot convert 705 to Point"},
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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

// Use github.com/shopspring/decimal as real-world database/sql custom type
// to test against.
func TestConnQueryDatabaseSQLScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	var num decimal.Decimal

	err := conn.QueryRow(context.Background(), "select '1234.567'::decimal").Scan(&num)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	expected, err := decimal.NewFromString("1234.567")
	if err != nil {
		t.Fatal(err)
	}

	if !num.Equals(expected) {
		t.Errorf("Expected num to be %v, but it was %v", expected, num)
	}

	ensureConnValid(t, conn)
}

// Use github.com/shopspring/decimal as real-world database/sql custom type
// to test against.
func TestConnQueryDatabaseSQLDriverValuer(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	expected, err := decimal.NewFromString("1234.567")
	if err != nil {
		t.Fatal(err)
	}
	var num decimal.Decimal

	err = conn.QueryRow(context.Background(), "select $1::decimal", &expected).Scan(&num)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if !num.Equals(expected) {
		t.Errorf("Expected num to be %v, but it was %v", expected, num)
	}

	ensureConnValid(t, conn)
}

// https://github.com/jackc/pgx/issues/339
func TestConnQueryDatabaseSQLDriverValuerWithAutoGeneratedPointerReceiver(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, "create temporary table t(n numeric)")

	var d *apd.Decimal
	commandTag, err := conn.Exec(context.Background(), `insert into t(n) values($1)`, d)
	if err != nil {
		t.Fatal(err)
	}
	if string(commandTag) != "INSERT 0 1" {
		t.Fatalf("want %s, got %s", "INSERT 0 1", commandTag)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryDatabaseSQLDriverValuerWithBinaryPgTypeThatAcceptsSameType(t *testing.T) {
	t.Parallel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	conn.ConnInfo().RegisterDataType(pgtype.DataType{
		Value: &gofrs.UUID{},
		Name:  "uuid",
		OID:   2950,
	})

	expected, err := uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	if err != nil {
		t.Fatal(err)
	}

	var u2 uuid.UUID
	err = conn.QueryRow(context.Background(), "select $1::uuid", expected).Scan(&u2)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if expected != u2 {
		t.Errorf("Expected u2 to be %v, but it was %v", expected, u2)
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

	skipCockroachDB(t, conn, "Server uses numeric instead of int")

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
		err := pgx.ScanRow(conn.ConnInfo(), resultReader.FieldDescriptions(), resultReader.Values(), &n)
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
			pgx.QuerySimpleProtocol(true),
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
			pgx.QuerySimpleProtocol(true),
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
			"select $1",
			pgx.QuerySimpleProtocol(true),
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
			pgx.QuerySimpleProtocol(true),
			expected,
		).Scan(&actual)
		if err != nil {
			t.Error(err)
		}
		if bytes.Compare(actual, expected) != 0 {
			t.Errorf("expected %v got %v", expected, actual)
		}
	}

	{
		expected := "test"
		var actual string
		err := conn.QueryRow(
			context.Background(),
			"select $1::text",
			pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
				pgx.QuerySimpleProtocol(true),
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
			expected := pgtype.Circle{P: pgtype.Vec2{1, 2}, R: 1.5, Status: pgtype.Present}
			actual := expected
			err := conn.QueryRow(
				context.Background(),
				"select $1::circle",
				pgx.QuerySimpleProtocol(true),
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
			"select $1::int8, $2::float8, $3, $4::bytea, $5::text",
			pgx.QuerySimpleProtocol(true),
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
		if bytes.Compare(expectedBytes, actualBytes) != 0 {
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
			pgx.QuerySimpleProtocol(true),
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

	skipCockroachDB(t, conn, "Server does not support changing client_encoding (https://www.cockroachlabs.com/docs/stable/set-vars.html)")

	mustExec(t, conn, "set client_encoding to 'SQL_ASCII'")

	var expected string
	err := conn.QueryRow(
		context.Background(),
		"select $1",
		pgx.QuerySimpleProtocol(true),
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

	skipCockroachDB(t, conn, "Server does not support standard_conforming_strings = off (https://github.com/cockroachdb/cockroach/issues/36215)")

	mustExec(t, conn, "set standard_conforming_strings to off")

	var expected string
	err := conn.QueryRow(
		context.Background(),
		"select $1",
		pgx.QuerySimpleProtocol(true),
		`\'; drop table users; --`,
	).Scan(&expected)
	if err == nil {
		t.Error("expected error when standard_conforming_strings is off, but no error occurred")
	}

	ensureConnValid(t, conn)
}

func TestQueryStatementCacheModes(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))

	tests := []struct {
		name                string
		buildStatementCache pgx.BuildStatementCacheFunc
	}{
		{
			name:                "disabled",
			buildStatementCache: nil,
		},
		{
			name: "prepare",
			buildStatementCache: func(conn *pgconn.PgConn) stmtcache.Cache {
				return stmtcache.New(conn, stmtcache.ModePrepare, 32)
			},
		},
		{
			name: "describe",
			buildStatementCache: func(conn *pgconn.PgConn) stmtcache.Cache {
				return stmtcache.New(conn, stmtcache.ModeDescribe, 32)
			},
		},
	}

	for _, tt := range tests {
		func() {
			config.BuildStatementCache = tt.buildStatementCache
			conn := mustConnect(t, config)
			defer closeConn(t, conn)

			var n int
			err := conn.QueryRow(context.Background(), "select 1").Scan(&n)
			assert.NoError(t, err, tt.name)
			assert.Equal(t, 1, n, tt.name)

			err = conn.QueryRow(context.Background(), "select 2").Scan(&n)
			assert.NoError(t, err, tt.name)
			assert.Equal(t, 2, n, tt.name)

			err = conn.QueryRow(context.Background(), "select 1").Scan(&n)
			assert.NoError(t, err, tt.name)
			assert.Equal(t, 1, n, tt.name)

			ensureConnValid(t, conn)
		}()
	}
}

// https://github.com/jackc/pgx/issues/895
func TestQueryErrorWithNilStatementCacheMode(t *testing.T) {
	t.Parallel()

	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = nil

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

func TestConnQueryFunc(t *testing.T) {
	t.Parallel()

	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, conn *pgx.Conn) {
		var actualResults []interface{}

		var a, b int
		ct, err := conn.QueryFunc(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			[]interface{}{3},
			[]interface{}{&a, &b},
			func(pgx.QueryFuncRow) error {
				actualResults = append(actualResults, []interface{}{a, b})
				return nil
			},
		)
		require.NoError(t, err)

		expectedResults := []interface{}{
			[]interface{}{1, 2},
			[]interface{}{2, 4},
			[]interface{}{3, 6},
		}
		require.Equal(t, expectedResults, actualResults)
		require.EqualValues(t, 3, ct.RowsAffected())
	})
}

func TestConnQueryFuncScanError(t *testing.T) {
	t.Parallel()

	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, conn *pgx.Conn) {
		var actualResults []interface{}

		var a, b int
		ct, err := conn.QueryFunc(
			context.Background(),
			"select 'foo', 'bar' from generate_series(1, $1) n",
			[]interface{}{3},
			[]interface{}{&a, &b},
			func(pgx.QueryFuncRow) error {
				actualResults = append(actualResults, []interface{}{a, b})
				return nil
			},
		)
		require.EqualError(t, err, "can't scan into dest[0]: unable to assign to *int")
		require.Nil(t, ct)
	})
}

func TestConnQueryFuncAbort(t *testing.T) {
	t.Parallel()

	testWithAndWithoutPreferSimpleProtocol(t, func(t *testing.T, conn *pgx.Conn) {
		var a, b int
		ct, err := conn.QueryFunc(
			context.Background(),
			"select n, n * 2 from generate_series(1, $1) n",
			[]interface{}{3},
			[]interface{}{&a, &b},
			func(pgx.QueryFuncRow) error {
				return errors.New("abort")
			},
		)
		require.EqualError(t, err, "abort")
		require.Nil(t, ct)
	})
}

func ExampleConn_QueryFunc() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	var a, b int
	_, err = conn.QueryFunc(
		context.Background(),
		"select n, n * 2 from generate_series(1, $1) n",
		[]interface{}{3},
		[]interface{}{&a, &b},
		func(pgx.QueryFuncRow) error {
			fmt.Printf("%v, %v\n", a, b)
			return nil
		},
	)
	if err != nil {
		fmt.Printf("QueryFunc error: %v", err)
		return
	}

	// Output:
	// 1, 2
	// 2, 4
	// 3, 6
}
