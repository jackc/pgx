package pgtype

import (
	"os"
	"testing"

	"github.com/jackc/pgx"
)

// func TestInt4rangeText(t *testing.T) {
// 	conns := mustConnectAll(t)
// 	defer mustCloseAll(t, conns)

// 	tests := []struct {
// 		name   string
// 		sql    string
// 		args   []interface{}
// 		err    error
// 		result Int4range
// 	}{
// 		{
// 			name:   "Normal",
// 			sql:    "select $1::int4range",
// 			args:   []interface{}{&Int4range{Lower: 1, Upper: 10, LowerType: Inclusive, UpperType: Exclusive}},
// 			err:    nil,
// 			result: Int4range{Lower: 1, Upper: 10, LowerType: Inclusive, UpperType: Exclusive},
// 		},
// 		{
// 			name:   "Negative",
// 			sql:    "select int4range(-42, -5)",
// 			args:   []interface{}{&Int4range{Lower: -42, Upper: -5, LowerType: Inclusive, UpperType: Exclusive}},
// 			err:    nil,
// 			result: Int4range{Lower: -42, Upper: -5, LowerType: Inclusive, UpperType: Exclusive},
// 		},
// 		{
// 			name:   "Normalized Bounds",
// 			sql:    "select int4range(1, 10, '(]')",
// 			args:   []interface{}{Int4range{Lower: 1, Upper: 10, LowerType: Exclusive, UpperType: Inclusive}},
// 			err:    nil,
// 			result: Int4range{Lower: 2, Upper: 11, LowerType: Inclusive, UpperType: Exclusive},
// 		},
// 	}

// 	for _, conn := range conns {
// 		for _, tt := range tests {
// 			var r Int4range
// 			var s string
// 			err := conn.QueryRow(tt.sql, tt.args...).Scan(&s)
// 			if err != tt.err {
// 				t.Errorf("%s %s: %v", conn.DriverName(), tt.name, err)
// 			}

// 			err = r.ParseText(s)
// 			if err != nil {
// 				t.Errorf("%s %s: %v", conn.DriverName(), tt.name, err)
// 			}

// 			if r != tt.result {
// 				t.Errorf("%s %s: expected %#v, got %#v", conn.DriverName(), tt.name, tt.result, r)
// 			}
// 		}
// 	}
// }

func TestInt4rangeParseText(t *testing.T) {
	conns := mustConnectAll(t)
	defer mustCloseAll(t, conns)

	tests := []struct {
		name   string
		sql    string
		args   []interface{}
		err    error
		result Int4range
	}{
		{
			name:   "Scan",
			sql:    "select int4range(1, 10)",
			args:   []interface{}{},
			err:    nil,
			result: Int4range{Lower: 1, Upper: 10, LowerType: Inclusive, UpperType: Exclusive},
		},
		{
			name:   "Scan Negative",
			sql:    "select int4range(-42, -5)",
			args:   []interface{}{},
			err:    nil,
			result: Int4range{Lower: -42, Upper: -5, LowerType: Inclusive, UpperType: Exclusive},
		},
		{
			name:   "Scan Normalized Bounds",
			sql:    "select int4range(1, 10, '(]')",
			args:   []interface{}{},
			err:    nil,
			result: Int4range{Lower: 2, Upper: 11, LowerType: Inclusive, UpperType: Exclusive},
		},
	}

	for _, conn := range conns {
		for _, tt := range tests {
			var r Int4range
			var s string
			err := conn.QueryRow(tt.sql, tt.args...).Scan(&s)
			if err != tt.err {
				t.Errorf("%s %s: %v", conn.DriverName(), tt.name, err)
			}

			err = r.ParseText(s)
			if err != nil {
				t.Errorf("%s %s: %v", conn.DriverName(), tt.name, err)
			}

			if r != tt.result {
				t.Errorf("%s %s: expected %#v, got %#v", conn.DriverName(), tt.name, tt.result, r)
			}
		}
	}
}

func TestInt4rangeParseBinary(t *testing.T) {
	config, err := pgx.ParseURI(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, conn)

	tests := []struct {
		name   string
		sql    string
		args   []interface{}
		err    error
		result Int4range
	}{
		{
			name:   "Scan",
			sql:    "select int4range(1, 10)",
			args:   []interface{}{},
			err:    nil,
			result: Int4range{Lower: 1, Upper: 10, LowerType: Inclusive, UpperType: Exclusive},
		},
		{
			name:   "Scan Negative",
			sql:    "select int4range(-42, -5)",
			args:   []interface{}{},
			err:    nil,
			result: Int4range{Lower: -42, Upper: -5, LowerType: Inclusive, UpperType: Exclusive},
		},
		{
			name:   "Scan Normalized Bounds",
			sql:    "select int4range(1, 10, '(]')",
			args:   []interface{}{},
			err:    nil,
			result: Int4range{Lower: 2, Upper: 11, LowerType: Inclusive, UpperType: Exclusive},
		},
	}

	for _, tt := range tests {
		ps, err := conn.Prepare(tt.sql, tt.sql)
		if err != nil {
			t.Errorf("conn.Prepare failed: %v", err)
			continue
		}
		ps.FieldDescriptions[0].FormatCode = pgx.BinaryFormatCode

		var r Int4range
		var buf []byte
		err = conn.QueryRow(tt.sql, tt.args...).Scan(&buf)
		if err != tt.err {
			t.Errorf("%s: %v", tt.name, err)
		}

		err = r.ParseBinary(buf)
		if err != nil {
			t.Errorf("%s: %v", tt.name, err)
		}

		if r != tt.result {
			t.Errorf("%s: expected %#v, got %#v", tt.name, tt.result, r)
		}
	}
}
