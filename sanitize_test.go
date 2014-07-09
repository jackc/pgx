package pgx_test

import (
	"github.com/jackc/pgx"
	"strings"
	"testing"
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
		{"select $1", []interface{}{42}, "select 42"},
		{"select $1", []interface{}{1.23}, "select 1.23"},
		{"select $1", []interface{}{true}, "select true"},
		{"select $1, $2, $3", []interface{}{"Jack's", 42, 1.23}, "select 'Jack''s', 42, 1.23"},
		{"select $1", []interface{}{[]byte{0, 15, 255, 17}}, `select E'\\x000fff11'`},
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
