package pgx

import (
	"testing"
)

func TestQuoteString(t *testing.T) {
	conn := getSharedConn()

	if conn.QuoteString("test") != "'test'" {
		t.Error("Failed to quote string")
	}

	if conn.QuoteString("Jack's") != "'Jack''s'" {
		t.Error("Failed to quote and escape string with embedded quote")
	}
}

func TestSanitizeSql(t *testing.T) {
	conn := getSharedConn()

	if conn.SanitizeSql("select $1, $2, $3", "Jack's", 42, 1.23) != "select 'Jack''s', 42, 1.23" {
		t.Error("Failed to sanitize sql")
	}
}
