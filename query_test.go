package pgx_test

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx"

	"github.com/shopspring/decimal"
)

func TestConnQueryScan(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var sum, rowCount int32

	rows, err := conn.Query("select generate_series(1,$1)", 10)
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

	rows, err := conn.Query("select 'foo', n, null, n::oid from generate_series(1,$1) n", 10)
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
		if len(values) != 4 {
			t.Errorf("Expected rows.Values to return 4 values, but it returned %d", len(values))
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
			t.Errorf(`Expected values[2] to be %v, but it was %d`, nil, values[2])
		}

		if values[3] != pgx.Oid(rowCount) {
			t.Errorf(`Expected values[3] to be %d, but it was %d`, rowCount, values[3])
		}
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", err)
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
		t.Fatalf("conn.Query failed: %v", err)
	}
	rows.Close()

	ensureConnValid(t, conn)

	// Read partial response then close
	rows, err = conn.Query("select generate_series(1,$1)", 10)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select 1/(10-n) from generate_series(1,10) n")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
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

	if rows.Err().Error() != "can't scan into dest[0]: Can't convert OID 23 to time.Time" {
		t.Fatalf("Expected different Rows.Err(): %v", rows.Err())
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select 1::int8, 2::int8, 3::int8")
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

func TestConnQueryScanner(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select null::int8, 1::int8")
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var n, m pgx.NullInt64
	err = rows.Scan(&n, &m)
	if err != nil {
		t.Fatalf("rows.Scan failed: %v", err)
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

func TestConnQueryErrorWhileReturningRows(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	for i := 0; i < 100; i++ {
		func() {
			sql := `select 42 / (random() * 20)::integer from generate_series(1,100000)`

			rows, err := conn.Query(sql)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			for rows.Next() {
				var n int32
				rows.Scan(&n)
			}

			if err, ok := rows.Err().(pgx.PgError); !ok {
				t.Fatalf("Expected pgx.PgError, got %v", err)
			}

			ensureConnValid(t, conn)
		}()
	}

}

func TestConnQueryEncoder(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	n := pgx.NullInt64{Int64: 1, Valid: true}

	rows, err := conn.Query("select $1::int8", &n)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	ok := rows.Next()
	if !ok {
		t.Fatal("rows.Next terminated early")
	}

	var m pgx.NullInt64
	err = rows.Scan(&m)
	if err != nil {
		t.Fatalf("rows.Scan failed: %v", err)
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

func TestQueryEncodeError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select $1::integer", "wrong")
	if err != nil {
		t.Errorf("conn.Query failure: %v", err)
	}
	defer rows.Close()

	rows.Next()

	if rows.Err() == nil {
		t.Error("Expected rows.Err() to return error, but it didn't")
	}
	if rows.Err().Error() != `ERROR: invalid input syntax for integer: "wrong" (SQLSTATE 22P02)` {
		t.Error("Expected rows.Err() to return different error:", rows.Err())
	}
}

// Ensure that an argument that implements Encoder works when the parameter type
// is a core type.
type coreEncoder struct{}

func (n coreEncoder) FormatCode() int16 { return pgx.TextFormatCode }

func (n *coreEncoder) Encode(w *pgx.WriteBuf, oid pgx.Oid) error {
	w.WriteInt32(int32(2))
	w.WriteBytes([]byte("42"))
	return nil
}

func TestQueryEncodeCoreTextFormatError(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var n int32
	err := conn.QueryRow("select $1::integer", &coreEncoder{}).Scan(&n)
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
		oid pgx.Oid
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
		{"select $1::oid", []interface{}{pgx.Oid(42)}, []interface{}{&actual.oid}, allTypes{oid: 42}},
	}

	for i, tt := range tests {
		actual = zero

		err := conn.QueryRow(tt.sql, tt.queryArgs...).Scan(tt.scanArgs...)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, queryArgs -> %v)", i, err, tt.sql, tt.queryArgs)
		}

		if actual != tt.expected {
			t.Errorf("%d. Expected %v, got %v (sql -> %v, queryArgs -> %v)", i, tt.expected, actual, tt.sql, tt.queryArgs)
		}

		ensureConnValid(t, conn)

		// Check that Scan errors when a core type is null
		err = conn.QueryRow(tt.sql, nil).Scan(tt.scanArgs...)
		if err == nil {
			t.Errorf("%d. Expected null to cause error, but it didn't (sql -> %v)", i, tt.sql)
		}
		if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
			t.Errorf(`%d. Expected null to cause error "Cannot decode null..." but it was %v (sql -> %v)`, i, err, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowCoreByteSlice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	tests := []struct {
		sql      string
		queryArg interface{}
		expected []byte
	}{
		{"select $1::text", "Jack", []byte("Jack")},
		{"select $1::text", []byte("Jack"), []byte("Jack")},
		{"select $1::int4", int32(239023409), []byte{14, 63, 53, 49}},
		{"select $1::varchar", []byte("Jack"), []byte("Jack")},
		{"select $1::bytea", []byte{0, 15, 255, 17}, []byte{0, 15, 255, 17}},
	}

	for i, tt := range tests {
		var actual []byte

		err := conn.QueryRow(tt.sql, tt.queryArg).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v)", i, err, tt.sql)
		}

		if bytes.Compare(actual, tt.expected) != 0 {
			t.Errorf("%d. Expected %v, got %v (sql -> %v)", i, tt.expected, actual, tt.sql)
		}

		ensureConnValid(t, conn)
	}
}

func TestQueryRowByteSliceArgument(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := "select $1::int4"
	queryArg := []byte{14, 63, 53, 49}
	expected := int32(239023409)

	var actual int32

	err := conn.QueryRow(sql, queryArg).Scan(&actual)
	if err != nil {
		t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
	}

	if expected != actual {
		t.Errorf("Expected %v, got %v (sql -> %v)", expected, actual, sql)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowUnknownType(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	sql := "select $1::point"
	expected := "(1,0)"
	var actual string

	err := conn.QueryRow(sql, expected).Scan(&actual)
	if err != nil {
		t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
	}

	if actual != expected {
		t.Errorf(`Expected "%v", got "%v" (sql -> %v)`, expected, actual, sql)

	}

	ensureConnValid(t, conn)
}

func TestQueryRowErrors(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
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
		{"select $1", []interface{}{"Jack"}, []interface{}{&actual.i16}, "could not determine data type of parameter $1 (SQLSTATE 42P18)"},
		{"select $1::badtype", []interface{}{"Jack"}, []interface{}{&actual.i16}, `type "badtype" does not exist`},
		{"SYNTAX ERROR", []interface{}{}, []interface{}{&actual.i16}, "SQLSTATE 42601"},
		{"select $1::text", []interface{}{"Jack"}, []interface{}{&actual.i16}, "Cannot decode oid 25 into int16"},
		{"select $1::point", []interface{}{int(705)}, []interface{}{&actual.s}, "cannot encode uint64 into oid 600"},
		{"select 42::int4", []interface{}{}, []interface{}{&actual.i}, "Scan cannot decode into *int"},
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

func TestQueryRowNoResults(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var n int32
	err := conn.QueryRow("select 1 where 1=0").Scan(&n)
	if err != pgx.ErrNoRows {
		t.Errorf("Expected pgx.ErrNoRows, got %v", err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowCoreInt16Slice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []int16

	tests := []struct {
		sql      string
		expected []int16
	}{
		{"select $1::int2[]", []int16{1, 2, 3, 4, 5}},
		{"select $1::int2[]", []int16{}},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.expected).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v", i, err)
		}

		if len(actual) != len(tt.expected) {
			t.Errorf("%d. Expected %v, got %v", i, tt.expected, actual)
		}

		for j := 0; j < len(actual); j++ {
			if actual[j] != tt.expected[j] {
				t.Errorf("%d. Expected actual[%d] to be %v, got %v", i, j, tt.expected[j], actual[j])
			}
		}

		ensureConnValid(t, conn)
	}

	// Check that Scan errors when an array with a null is scanned into a core slice type
	err := conn.QueryRow("select '{1, 2, 3, 4, 5, null}'::int2[];").Scan(&actual)
	if err == nil {
		t.Error("Expected null to cause error when scanned into slice, but it didn't")
	}
	if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
		t.Errorf(`Expected null to cause error "Cannot decode null..." but it was %v`, err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowCoreInt32Slice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []int32

	tests := []struct {
		sql      string
		expected []int32
	}{
		{"select $1::int4[]", []int32{1, 2, 3, 4, 5}},
		{"select $1::int4[]", []int32{}},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.expected).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v", i, err)
		}

		if len(actual) != len(tt.expected) {
			t.Errorf("%d. Expected %v, got %v", i, tt.expected, actual)
		}

		for j := 0; j < len(actual); j++ {
			if actual[j] != tt.expected[j] {
				t.Errorf("%d. Expected actual[%d] to be %v, got %v", i, j, tt.expected[j], actual[j])
			}
		}

		ensureConnValid(t, conn)
	}

	// Check that Scan errors when an array with a null is scanned into a core slice type
	err := conn.QueryRow("select '{1, 2, 3, 4, 5, null}'::int4[];").Scan(&actual)
	if err == nil {
		t.Error("Expected null to cause error when scanned into slice, but it didn't")
	}
	if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
		t.Errorf(`Expected null to cause error "Cannot decode null..." but it was %v`, err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowCoreInt64Slice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []int64

	tests := []struct {
		sql      string
		expected []int64
	}{
		{"select $1::int8[]", []int64{1, 2, 3, 4, 5}},
		{"select $1::int8[]", []int64{}},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.expected).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v", i, err)
		}

		if len(actual) != len(tt.expected) {
			t.Errorf("%d. Expected %v, got %v", i, tt.expected, actual)
		}

		for j := 0; j < len(actual); j++ {
			if actual[j] != tt.expected[j] {
				t.Errorf("%d. Expected actual[%d] to be %v, got %v", i, j, tt.expected[j], actual[j])
			}
		}

		ensureConnValid(t, conn)
	}

	// Check that Scan errors when an array with a null is scanned into a core slice type
	err := conn.QueryRow("select '{1, 2, 3, 4, 5, null}'::int8[];").Scan(&actual)
	if err == nil {
		t.Error("Expected null to cause error when scanned into slice, but it didn't")
	}
	if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
		t.Errorf(`Expected null to cause error "Cannot decode null..." but it was %v`, err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowCoreFloat32Slice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []float32

	tests := []struct {
		sql      string
		expected []float32
	}{
		{"select $1::float4[]", []float32{1.5, 2.0, 3.5}},
		{"select $1::float4[]", []float32{}},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.expected).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v", i, err)
		}

		if len(actual) != len(tt.expected) {
			t.Errorf("%d. Expected %v, got %v", i, tt.expected, actual)
		}

		for j := 0; j < len(actual); j++ {
			if actual[j] != tt.expected[j] {
				t.Errorf("%d. Expected actual[%d] to be %v, got %v", i, j, tt.expected[j], actual[j])
			}
		}

		ensureConnValid(t, conn)
	}

	// Check that Scan errors when an array with a null is scanned into a core slice type
	err := conn.QueryRow("select '{1.5, 2.0, 3.5, null}'::float4[];").Scan(&actual)
	if err == nil {
		t.Error("Expected null to cause error when scanned into slice, but it didn't")
	}
	if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
		t.Errorf(`Expected null to cause error "Cannot decode null..." but it was %v`, err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowCoreFloat64Slice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []float64

	tests := []struct {
		sql      string
		expected []float64
	}{
		{"select $1::float8[]", []float64{1.5, 2.0, 3.5}},
		{"select $1::float8[]", []float64{}},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.expected).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v", i, err)
		}

		if len(actual) != len(tt.expected) {
			t.Errorf("%d. Expected %v, got %v", i, tt.expected, actual)
		}

		for j := 0; j < len(actual); j++ {
			if actual[j] != tt.expected[j] {
				t.Errorf("%d. Expected actual[%d] to be %v, got %v", i, j, tt.expected[j], actual[j])
			}
		}

		ensureConnValid(t, conn)
	}

	// Check that Scan errors when an array with a null is scanned into a core slice type
	err := conn.QueryRow("select '{1.5, 2.0, 3.5, null}'::float8[];").Scan(&actual)
	if err == nil {
		t.Error("Expected null to cause error when scanned into slice, but it didn't")
	}
	if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
		t.Errorf(`Expected null to cause error "Cannot decode null..." but it was %v`, err)
	}

	ensureConnValid(t, conn)
}

func TestQueryRowCoreStringSlice(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var actual []string

	tests := []struct {
		sql      string
		expected []string
	}{
		{"select $1::text[]", []string{"Adam", "Eve", "UTF-8 Characters Å Æ Ë Ͽ"}},
		{"select $1::text[]", []string{}},
		{"select $1::varchar[]", []string{"Adam", "Eve", "UTF-8 Characters Å Æ Ë Ͽ"}},
		{"select $1::varchar[]", []string{}},
	}

	for i, tt := range tests {
		err := conn.QueryRow(tt.sql, tt.expected).Scan(&actual)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v", i, err)
		}

		if len(actual) != len(tt.expected) {
			t.Errorf("%d. Expected %v, got %v", i, tt.expected, actual)
		}

		for j := 0; j < len(actual); j++ {
			if actual[j] != tt.expected[j] {
				t.Errorf("%d. Expected actual[%d] to be %v, got %v", i, j, tt.expected[j], actual[j])
			}
		}

		ensureConnValid(t, conn)
	}

	// Check that Scan errors when an array with a null is scanned into a core slice type
	err := conn.QueryRow("select '{Adam,Eve,NULL}'::text[];").Scan(&actual)
	if err == nil {
		t.Error("Expected null to cause error when scanned into slice, but it didn't")
	}
	if err != nil && !strings.Contains(err.Error(), "Cannot decode null") {
		t.Errorf(`Expected null to cause error "Cannot decode null..." but it was %v`, err)
	}

	ensureConnValid(t, conn)
}

func TestReadingValueAfterEmptyArray(t *testing.T) {
	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var a []string
	var b int32
	err := conn.QueryRow("select '{}'::text[], 42::integer").Scan(&a, &b)
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
	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var a []byte
	err := conn.QueryRow("select null::text").Scan(&a)
	if err != nil {
		t.Fatalf("conn.QueryRow failed: %v", err)
	}

	if a != nil {
		t.Errorf("Expected 'a' to be nil, but it was: %v", a)
	}
}

func TestReadingNullByteArrays(t *testing.T) {
	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	rows, err := conn.Query("select null::text union all select null::text")
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	var num decimal.Decimal

	err := conn.QueryRow("select '1234.567'::decimal").Scan(&num)
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

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	expected, err := decimal.NewFromString("1234.567")
	if err != nil {
		t.Fatal(err)
	}
	var num decimal.Decimal

	err = conn.QueryRow("select $1::decimal", expected).Scan(&num)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if !num.Equals(expected) {
		t.Errorf("Expected num to be %v, but it was %v", expected, num)
	}

	ensureConnValid(t, conn)
}

func TestConnQueryDatabaseSQLNullX(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
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

func TestConnQueryWithQueryExecTimeoutSet(t *testing.T) {
	t.Parallel()

	config := *defaultConnConfig

	// case 1: too small timeout to run a query
	config.QueryExecTimeout = 500 * time.Millisecond
	conn1 := mustConnect(t, config)
	defer closeConn(t, conn1)

	rows, err := conn1.Query("SELECT pg_sleep(2)")
	if err != nil {
		t.Fatalf("Expected Query() not to fail, instead it failed with '%v'", err)
	}

	hasNext := rows.Next()
	if hasNext {
		t.Fatal("Expected rows.Next() to return false, instead it did not")
	}
	if rows.Err() == nil {
		t.Fatal("Expected Query() to fail with 'conn is dead', instead it did not")
	}
	if !strings.Contains(rows.Err().Error(), "terminating connection") {
		t.Fatalf("Expected Query() to fail with timeout, instead it failed with '%v'", rows.Err())
	}

	// case 2: big enough timeout that allows query to finish.
	config.QueryExecTimeout = 10 * time.Second
	conn2 := mustConnect(t, config)
	defer closeConn(t, conn2)

	rows, err = conn2.Query("SELECT pg_sleep(2)")
	if err != nil {
		t.Fatalf("Expected Query() not to fail, instead it failed with '%v'", err)
	}

	hasNext = rows.Next()
	if !hasNext {
		t.Fatal("Expected rows.Next() to return true, instead it did not")
	}
	if rows.Err() != nil {
		t.Fatalf("Expected rows.Next() not to fail, instead it failed with '%v'", rows.Err())
	}
}
