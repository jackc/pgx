package pgx_test

import (
	"testing"
)

func TestQuoteString(t *testing.T) {
	conn := getSharedConnection(t)

	if conn.QuoteString("test") != "'test'" {
		t.Error("Failed to quote string")
	}

	if conn.QuoteString("Jack's") != "'Jack''s'" {
		t.Error("Failed to quote and escape string with embedded quote")
	}
}

func TestSanitizeSql(t *testing.T) {
	conn := getSharedConnection(t)

	if san, err := conn.SanitizeSql("select $1", "Jack's"); err != nil || san != "select 'Jack''s'" {
		t.Errorf("Failed to sanitize string: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1", 42); err != nil || san != "select 42" {
		t.Errorf("Failed to pass through integer: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1", 1.23); err != nil || san != "select 1.23" {
		t.Errorf("Failed to pass through float: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1, $2, $3", "Jack's", 42, 1.23); err != nil || san != "select 'Jack''s', 42, 1.23" {
		t.Errorf("Failed to sanitize multiple params: %v - %v", san, err)
	}

	bytea := make([]byte, 4)
	bytea[0] = 0   // 0x00
	bytea[1] = 15  // 0x0F
	bytea[2] = 255 // 0xFF
	bytea[3] = 17  // 0x11

	if san, err := conn.SanitizeSql("select $1", bytea); err != nil || san != `select E'\\x000fff11'` {
		t.Errorf("Failed to sanitize []byte: %v - %v", san, err)
	}
}
