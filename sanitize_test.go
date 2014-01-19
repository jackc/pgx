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

	if san, err := conn.SanitizeSql("select $1", nil); err != nil || san != "select null" {
		t.Errorf("Failed to translate nil to null: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1", "Jack's"); err != nil || san != "select 'Jack''s'" {
		t.Errorf("Failed to sanitize string: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1", 42); err != nil || san != "select 42" {
		t.Errorf("Failed to pass through integer: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1", 1.23); err != nil || san != "select 1.23" {
		t.Errorf("Failed to pass through float: %v - %v", san, err)
	}

	if san, err := conn.SanitizeSql("select $1", true); err != nil || san != "select true" {
		t.Errorf("Failed to pass through bool: %v - %v", san, err)
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

	int2a := make([]int16, 4)
	int2a[0] = 42
	int2a[1] = 0
	int2a[2] = -1
	int2a[3] = 32123

	if san, err := conn.SanitizeSql("select $1::int2[]", int2a); err != nil || san != `select '{42,0,-1,32123}'::int2[]` {
		t.Errorf("Failed to sanitize []int16: %v - %v", san, err)
	}

	int4a := make([]int32, 4)
	int4a[0] = 42
	int4a[1] = 0
	int4a[2] = -1
	int4a[3] = 32123

	if san, err := conn.SanitizeSql("select $1::int4[]", int4a); err != nil || san != `select '{42,0,-1,32123}'::int4[]` {
		t.Errorf("Failed to sanitize []int32: %v - %v", san, err)
	}

	int8a := make([]int64, 4)
	int8a[0] = 42
	int8a[1] = 0
	int8a[2] = -1
	int8a[3] = 32123

	if san, err := conn.SanitizeSql("select $1::int8[]", int8a); err != nil || san != `select '{42,0,-1,32123}'::int8[]` {
		t.Errorf("Failed to sanitize []int64: %v - %v", san, err)
	}
}
