package pgx_test

import (
	"fmt"
	"github.com/jackc/pgx"
	"strings"
	"testing"
	"time"
)

func TestQuoteString(t *testing.T) {
	t.Parallel()

	if pgx.QuoteString("test") != "'test'" {
		t.Error("Failed to quote string")
	}

	if pgx.QuoteString("Jack's") != "'Jack''s'" {
		t.Error("Failed to quote and escape string with embedded quote")
	}
}

func TestSanitizeSql(t *testing.T) {
	t.Parallel()

	successTests := []struct {
		sql    string
		args   []interface{}
		output string
	}{
		{"select $1", []interface{}{nil}, "select null"},
		{"select $1", []interface{}{"Jack's"}, "select 'Jack''s'"},
		{"select $1", []interface{}{int(42)}, "select 42"},
		{"select $1", []interface{}{uint(42)}, "select 42"},
		{"select $1", []interface{}{int8(42)}, "select 42"},
		{"select $1", []interface{}{int16(42)}, "select 42"},
		{"select $1", []interface{}{int32(42)}, "select 42"},
		{"select $1", []interface{}{int64(42)}, "select 42"},
		{"select $1", []interface{}{uint8(42)}, "select 42"},
		{"select $1", []interface{}{uint16(42)}, "select 42"},
		{"select $1", []interface{}{uint32(42)}, "select 42"},
		{"select $1", []interface{}{uint64(42)}, "select 42"},
		{"select $1", []interface{}{float32(1.23)}, "select 1.23"},
		{"select $1", []interface{}{float64(1.23)}, "select 1.23"},
		{"select $1", []interface{}{true}, "select true"},
		{"select $1, $2, $3", []interface{}{"Jack's", 42, 1.23}, "select 'Jack''s', 42, 1.23"},
		{"select $1", []interface{}{[]byte{0, 15, 255, 17}}, `select E'\\x000fff11'`},
		{"select $1", []interface{}{&pgx.NullInt64{Int64: 0, Valid: false}}, "select null"},
		{"select $1", []interface{}{&pgx.NullInt64{Int64: 1, Valid: true}}, "select 1"},
	}

	for i, tt := range successTests {
		san, err := pgx.SanitizeSql(tt.sql, tt.args...)
		if err != nil {
			t.Errorf("%d. Unexpected failure: %v (sql -> %v, args -> %v)", i, err, tt.sql, tt.args)
		}
		if san != tt.output {
			t.Errorf("%d. Expected %v, got %v (sql -> %v, args -> %v)", i, tt.output, san, tt.sql, tt.args)
		}
	}

	errorTests := []struct {
		sql  string
		args []interface{}
		err  string
	}{
		{"select $1", []interface{}{t}, "is not a core type and it does not implement TextEncoder"},
		{"select $1, $2", []interface{}{}, "Cannot interpolate $1, only 0 arguments provided"},
	}

	for i, tt := range errorTests {
		_, err := pgx.SanitizeSql(tt.sql, tt.args...)
		if err == nil {
			t.Errorf("%d. Unexpected success (sql -> %v, args -> %v)", i, tt.sql, tt.args, err)
		}
		if !strings.Contains(err.Error(), tt.err) {
			t.Errorf("%d. Expected error to contain %s, but got %v (sql -> %v, args -> %v)", i, tt.err, err, tt.sql, tt.args)
		}
	}
}

func TestDateTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	mustPrepare(t, conn, "testTranscode", "select $1::date")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	dates := []time.Time{
		time.Date(1990, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local),
		time.Date(2001, 1, 2, 0, 0, 0, 0, time.Local),
		time.Date(2004, 2, 29, 0, 0, 0, 0, time.Local),
		time.Date(2013, 7, 4, 0, 0, 0, 0, time.Local),
		time.Date(2013, 12, 25, 0, 0, 0, 0, time.Local),
	}

	for _, actualDate := range dates {
		var d time.Time

		// Test text format
		err := conn.QueryRow("select $1::date", actualDate).Scan(&d)
		if err != nil {
			t.Fatalf("Unexpected failure on QueryRow Scan: %v", err)
		}
		if !actualDate.Equal(d) {
			t.Errorf("Did not transcode date successfully: %v is not %v", d, actualDate)
		}

		// Test binary format
		err = conn.QueryRow("testTranscode", actualDate).Scan(&d)
		if err != nil {
			t.Fatalf("Unexpected failure on QueryRow Scan: %v", err)
		}
		if !actualDate.Equal(d) {
			t.Errorf("Did not transcode date successfully: %v is not %v", d, actualDate)
		}
	}
}

func TestTimestampTzTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	inputTime := time.Date(2013, 1, 2, 3, 4, 5, 6000, time.Local)

	var outputTime time.Time

	err := conn.QueryRow("select $1::timestamptz", inputTime).Scan(&outputTime)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}

	mustPrepare(t, conn, "testTranscode", "select $1::timestamptz")
	defer func() {
		if err := conn.Deallocate("testTranscode"); err != nil {
			t.Fatalf("Unable to deallocate prepared statement: %v", err)
		}
	}()

	err = conn.QueryRow("testTranscode", inputTime).Scan(&outputTime)
	if err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if !inputTime.Equal(outputTime) {
		t.Errorf("Did not transcode time successfully: %v is not %v", outputTime, inputTime)
	}
}

func TestNullX(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   pgx.NullString
		i16 pgx.NullInt16
		i32 pgx.NullInt32
		i64 pgx.NullInt64
		f32 pgx.NullFloat32
		f64 pgx.NullFloat64
		b   pgx.NullBool
		t   pgx.NullTime
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		expected  allTypes
	}{
		{"select $1::text", []interface{}{pgx.NullString{String: "foo", Valid: true}}, []interface{}{&actual.s}, allTypes{s: pgx.NullString{String: "foo", Valid: true}}},
		{"select $1::text", []interface{}{pgx.NullString{String: "foo", Valid: false}}, []interface{}{&actual.s}, allTypes{s: pgx.NullString{String: "", Valid: false}}},
		{"select $1::int2", []interface{}{pgx.NullInt16{Int16: 1, Valid: true}}, []interface{}{&actual.i16}, allTypes{i16: pgx.NullInt16{Int16: 1, Valid: true}}},
		{"select $1::int2", []interface{}{pgx.NullInt16{Int16: 1, Valid: false}}, []interface{}{&actual.i16}, allTypes{i16: pgx.NullInt16{Int16: 0, Valid: false}}},
		{"select $1::int4", []interface{}{pgx.NullInt32{Int32: 1, Valid: true}}, []interface{}{&actual.i32}, allTypes{i32: pgx.NullInt32{Int32: 1, Valid: true}}},
		{"select $1::int4", []interface{}{pgx.NullInt32{Int32: 1, Valid: false}}, []interface{}{&actual.i32}, allTypes{i32: pgx.NullInt32{Int32: 0, Valid: false}}},
		{"select $1::int8", []interface{}{pgx.NullInt64{Int64: 1, Valid: true}}, []interface{}{&actual.i64}, allTypes{i64: pgx.NullInt64{Int64: 1, Valid: true}}},
		{"select $1::int8", []interface{}{pgx.NullInt64{Int64: 1, Valid: false}}, []interface{}{&actual.i64}, allTypes{i64: pgx.NullInt64{Int64: 0, Valid: false}}},
		{"select $1::float4", []interface{}{pgx.NullFloat32{Float32: 1.23, Valid: true}}, []interface{}{&actual.f32}, allTypes{f32: pgx.NullFloat32{Float32: 1.23, Valid: true}}},
		{"select $1::float4", []interface{}{pgx.NullFloat32{Float32: 1.23, Valid: false}}, []interface{}{&actual.f32}, allTypes{f32: pgx.NullFloat32{Float32: 0, Valid: false}}},
		{"select $1::float8", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: true}}, []interface{}{&actual.f64}, allTypes{f64: pgx.NullFloat64{Float64: 1.23, Valid: true}}},
		{"select $1::float8", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: false}}, []interface{}{&actual.f64}, allTypes{f64: pgx.NullFloat64{Float64: 0, Valid: false}}},
		{"select $1::bool", []interface{}{pgx.NullBool{Bool: true, Valid: true}}, []interface{}{&actual.b}, allTypes{b: pgx.NullBool{Bool: true, Valid: true}}},
		{"select $1::bool", []interface{}{pgx.NullBool{Bool: true, Valid: false}}, []interface{}{&actual.b}, allTypes{b: pgx.NullBool{Bool: false, Valid: false}}},
		{"select $1::timestamptz", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}},
		{"select $1::timestamptz", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: false}}, []interface{}{&actual.t}, allTypes{t: pgx.NullTime{Time: time.Time{}, Valid: false}}},
		{"select 42::int4, $1::float8", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: true}}, []interface{}{&actual.i32, &actual.f64}, allTypes{i32: pgx.NullInt32{Int32: 42, Valid: true}, f64: pgx.NullFloat64{Float64: 1.23, Valid: true}}},
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
		}
	}
}

func TestNullXMismatch(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type allTypes struct {
		s   pgx.NullString
		i16 pgx.NullInt16
		i32 pgx.NullInt32
		i64 pgx.NullInt64
		f32 pgx.NullFloat32
		f64 pgx.NullFloat64
		b   pgx.NullBool
		t   pgx.NullTime
	}

	var actual, zero allTypes

	tests := []struct {
		sql       string
		queryArgs []interface{}
		scanArgs  []interface{}
		err       string
	}{
		{"select $1::date", []interface{}{pgx.NullString{String: "foo", Valid: true}}, []interface{}{&actual.s}, "invalid input syntax for type date"},
		{"select $1::date", []interface{}{pgx.NullInt16{Int16: 1, Valid: true}}, []interface{}{&actual.i16}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullInt32{Int32: 1, Valid: true}}, []interface{}{&actual.i32}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullInt64{Int64: 1, Valid: true}}, []interface{}{&actual.i64}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullFloat32{Float32: 1.23, Valid: true}}, []interface{}{&actual.f32}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullFloat64{Float64: 1.23, Valid: true}}, []interface{}{&actual.f64}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullBool{Bool: true, Valid: true}}, []interface{}{&actual.b}, "cannot encode into OID 1082"},
		{"select $1::date", []interface{}{pgx.NullTime{Time: time.Unix(123, 5000), Valid: true}}, []interface{}{&actual.t}, "cannot encode into OID 1082"},
	}

	for i, tt := range tests {
		psName := fmt.Sprintf("ps%d", i)
		mustPrepare(t, conn, psName, tt.sql)

		actual = zero

		err := conn.QueryRow(psName, tt.queryArgs...).Scan(tt.scanArgs...)
		if err == nil || !strings.Contains(err.Error(), tt.err) {
			t.Errorf(`%d. Expected error to contain "%s", but it didn't: %v`, i, tt.err, err)
		}

		ensureConnValid(t, conn)
	}
}
