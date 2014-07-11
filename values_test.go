package pgx_test

import (
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

// TODO
func TestNilTranscode(t *testing.T) {
	// t.Parallel()

	// conn := mustConnect(t, *defaultConnConfig)
	// defer closeConn(t, conn)

	// var inputNil interface{}
	// inputNil = nil

	// result := mustSelectValue(t, conn, "select $1::integer", inputNil)
	// if result != nil {
	// 	t.Errorf("Did not transcode nil successfully for normal query: %v", result)
	// }

	// mustPrepare(t, conn, "testTranscode", "select $1::integer")
	// defer func() {
	// 	if err := conn.Deallocate("testTranscode"); err != nil {
	// 		t.Fatalf("Unable to deallocate prepared statement: %v", err)
	// 	}
	// }()

	// result = mustSelectValue(t, conn, "testTranscode", inputNil)
	// if result != nil {
	// 	t.Errorf("Did not transcode nil successfully for prepared query: %v", result)
	// }
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
