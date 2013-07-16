package pgx_test

import (
	"testing"
)

func TestQuoteString(t *testing.T) {
	conn := GetSharedConnection()

	if conn.QuoteString("test") != "'test'" {
		t.Error("Failed to quote string")
	}

	if conn.QuoteString("Jack's") != "'Jack''s'" {
		t.Error("Failed to quote and escape string with embedded quote")
	}
}

func TestSanitizeSql(t *testing.T) {
	conn := GetSharedConnection()

	if conn.SanitizeSql("select $1", "Jack's") != "select 'Jack''s'" {
		t.Error("Failed to sanitize string")
	}

	if conn.SanitizeSql("select $1", 42) != "select 42" {
		t.Error("Failed to pass through integer")
	}

	if conn.SanitizeSql("select $1", 1.23) != "select 1.23" {
		t.Error("Failed to pass through float")
	}

	if conn.SanitizeSql("select $1, $2, $3", "Jack's", 42, 1.23) != "select 'Jack''s', 42, 1.23" {
		t.Error("Failed to sanitize multiple params")
	}

	bytea := make([]byte, 4)
	bytea[0] = 0   // 0x00
	bytea[1] = 15  // 0x0F
	bytea[2] = 255 // 0xFF
	bytea[3] = 17  // 0x11

	if conn.SanitizeSql("select $1", bytea) != `select E'\\x000fff11'` {
		t.Error("Failed to sanitize []byte")
	}
}
