package pgx_test

import (
	"bytes"
	"fmt"
	"github.com/jackc/pgx"
	"strings"
	"testing"
	"time"
)

func TestConnQueryScan(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var sum, rowCount int32

	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

func TestConnQueryValues(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var rowCount int32

	rows, err := conn.Query("select 'foo', n, null from generate_series(1,$1) n", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	defer rows.Close()

	for rows.Next() {
		rowCount++

		values, err := rows.Values()
		if err != nil {
			t.Fatalf("rows.Values failed: %v", err)
		}
		if len(values) != 3 {
			t.Errorf("Expected rows.Values to return 3 values, but it returned %d", len(values))
		}
		if values[0] != "foo" {
			t.Errorf(`Expected values[0] to be "foo", but it was %v`, values[0])
		}
		if values[0] != "foo" {
			t.Errorf(`Expected values[0] to be "foo", but it was %v`, values[0])
		}

		if values[1] != rowCount {
			t.Errorf(`Expected values[1] to be %d, but it was %d`, rowCount, values[1])
		}

		if values[2] != nil {
			t.Errorf(`Expected values[2] to be %d, but it was %d`, nil, values[2])
		}
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
}

// Test that a connection stays valid when query results are closed early
func TestConnQueryCloseEarly(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Immediately close query without reading any rows
	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}
	rows.Close()

	ensureConnValid(t, conn)

	// Read partial response then close
	rows, err = conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
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

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadWrongTypeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Read a single value incorrectly
	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
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

	ensureConnValid(t, conn)
}

// Test that a connection stays valid when query results read incorrectly
func TestConnQueryReadTooManyValues(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	// Read too many values
	rows, err := conn.Query("select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
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

func TestConnQueryUnpreparedScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select null::int8, 1::int8")
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var n, m pgx.NullInt64
	err = rows.Scan(&n, &m)
	if err != nil {
		t.Fatalf("rows.Scan failed: ", err)
	}
	rows.Close()

	if n.Valid {
		t.Error("Null should not be valid, but it was")
	}

	if !m.Valid {
		t.Error("1 should be valid, but it wasn't")
	}

	if m.Int64 != 1 {
		t.Errorf("m.Int64 should have been 1, but it was %v", m.Int64)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryPreparedScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "scannerTest", "select null::int8, 1::int8")

	rows, err := conn.Query("scannerTest")
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var n, m pgx.NullInt64
	err = rows.Scan(&n, &m)
	if err != nil {
		t.Fatalf("rows.Scan failed: ", err)
	}
	rows.Close()

	if n.Valid {
		t.Error("Null should not be valid, but it was")
	}

	if !m.Valid {
		t.Error("1 should be valid, but it wasn't")
	}

	if m.Int64 != 1 {
		t.Errorf("m.Int64 should have been 1, but it was %v", m.Int64)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryUnpreparedEncoder(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	n := pgx.NullInt64{Int64: 1, Valid: true}

	rows, err := conn.Query("select $1::int8", &n)
	if err != nil {
		t.Fatalf("conn.Query failed: ", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var m pgx.NullInt64
	err = rows.Scan(&m)
	if err != nil {
		t.Fatalf("rows.Scan failed: ", err)
	}
	rows.Close()

	if !m.Valid {
		t.Error("m should be valid, but it wasn't")
	}

	if m.Int64 != 1 {
		t.Errorf("m.Int64 should have been 1, but it was %v", m.Int64)
	}

	ensureConnValid(t, conn)
}

func TestQueryPreparedEncodeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "testTranscode", "select $1::integer")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	_, err := conn.Query("testTranscode", "wrong")
	switch {
	case err == nil:
		t.Error("Expected transcode error to return error, but it didn't")
	case err.Error() == "Expected integer representable in int32, received string wrong":
		// Correct behavior
	default:
		t.Errorf("Expected transcode error, received %v", err)
	}
}

// Ensure that an argument that implements TextEncoder, but not BinaryEncoder
// works when the parameter type is a core type.
type coreTextEncoder struct{}

func (n *coreTextEncoder) EncodeText() (string, byte, error) {
	return "42", pgx.SafeText, nil
}

func TestQueryPreparedEncodeCoreTextFormatError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "testTranscode", "select $1::integer")

	var n int32
	err := conn.QueryRow("testTranscode", &coreTextEncoder{}).Scan(&n)
	if err != nil {
		t.Fatalf("Unexpected conn.QueryRow error: %v", err)
	}

	if n != 42 {
		t.Errorf("Expected 42, got %v", n)
	}
}

func TestQueryRowCoreTypes(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   string
		i16 int16
		i32 int32
		i64 int64
		f32 float32
		f64 float64
		b   bool
		t   time.Time
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		expected  allTypes
	}{
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.s}, allTypes{s: "Jack"}},
		{"select $1::int2", []interface{}{int16(42)}, []interface{}{&actual.i16}, allTypes{i16: 42}},
		{"select $1::int4", []interface{}{int32(42)}, []interface{}{&actual.i32}, allTypes{i32: 42}},
		{"select $1::int8", []interface{}{int64(42)}, []interface{}{&actual.i64}, allTypes{i64: 42}},
		{"select $1::float4", []interface{}{float32(1.23)}, []interface{}{&actual.f32}, allTypes{f32: 1.23}},
		{"select $1::float8", []interface{}{float64(1.23)}, []interface{}{&actual.f64}, allTypes{f64: 1.23}},
		{"select $1::bool", []interface{}{true}, []interface{}{&actual.b}, allTypes{b: true}},
		{"select $1::timestamptz", []interface{}{time.Unix(123, 5000)}, []interface{}{&actual.t}, allTypes{t: time.Unix(123, 5000)}},
		{"select $1::timestamp", []interface{}{time.Date(2010, 1, 2, 3, 4, 5, 0, time.Local)}, []interface{}{&actual.t}, allTypes{t: time.Date(2010, 1, 2, 3, 4, 5, 0, time.Local)}},
		{"select $1::date", []interface{}{time.Date(1987, 1, 2, 0, 0, 0, 0, time.Local)}, []interface{}{&actual.t}, allTypes{t: time.Date(1987, 1, 2, 0, 0, 0, 0, time.Local)}},
	}

	for i, tt := range tests {
		psName := fmt.Sprintf("success%d", i)
		mustPrepare(t, conn, psName, tt.sql)

		for _, sql := range []string{tt.sql, psName} {
			actual = zero

			err := conn.QueryRow(sql, tt.queryArgs...).Scan(tt.scanArgs...)
			if err != nil {
				t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, sql, tt.queryArgs)
			}

			if actual != tt.expected {
				t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArgs -> %v)", i, tt.expected, actual, sql, tt.queryArgs)
			}

			ensureConnValid(t, conn)

			// Check that Scan errors when a core type is null
			err = conn.QueryRow(sql, nil).Scan(tt.scanArgs...)
			if err == nil {
				t.Errorf("%d. Expected null to cause error, but it didn't (sql -> %v)", i, sql)
			}
			if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
				t.Errorf(`%d. Expected null to cause error "Cannot decode null..." but it was %v (sql -> %v)`, i, err, sql)
			}
		}
	}
}

func TestQueryRowCoreBytea(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []byte
	sql := "select $1::bytea"
	queryArg := []byte{0, 15, 255, 17}
	expected := []byte{0, 15, 255, 17}

	psName := "selectBytea"
	mustPrepare(t, conn, psName, sql)

	for _, sql := range []string{sql, psName} {
		actual = nil

		err := conn.QueryRow(sql, queryArg).Scan(&actual)
		if err != nil {
			t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
		}

		if bytes.Compare(actual, expected) != 0 {
			t.Errorf("Expected %v, got %v (sql -> %v)", expected, actual, sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowUnpreparedErrors(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		i16 int16
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		{"select $1", []interface{}{"Jack"}, []interface{}{&actual.i16}, "Expected type oid 21 but received type oid 705"},
		{"select $1::badtype", []interface{}{"Jack"}, []interface{}{&actual.i16}, `type "badtype" does not exist`},
		{"SYNTAX ERROR", []interface{}{}, []interface{}{&actual.i16}, "SQLSTATE 42601"},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err == nil {
			t.Errorf("%d. Unexpected success (sql -> %v, queryArgs -> %v)", i, tt.sql, tt.queryArgs)
		}
		if err != nil && !strings.Contains(err.Error(), tt.err) {
			t.Errorf("%d. Expected error to contain %s, but got %v (sql -> %v, queryArgs -> %v)", i, tt.err, err, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowPreparedErrors(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		i16 int16
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.i16}, "Expected type oid 21 but received type oid 25"},
	}

	for i, tt := range tests {
		psName := fmt.Sprintf("ps%d", i)
		mustPrepare(t, conn, psName, tt.sql)

		actual = zero

		err := conn.QueryRow(psName, tt.queryArgs...).Scan(tt.scanArgs...)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := "select 1 where 1=0"
	psName := "selectNothing"
	mustPrepare(t, conn, psName, sql)

	for _, sql := range []string{sql, psName} {
		var n int32
		err := conn.QueryRow(sql).Scan(&n)
		if err != pgx.ErrNoRows {
			t.Errorf("Expected pgx.ErrNoRows, got %v", err)
		}

		ensureConnValid(t, conn)
	}
}
