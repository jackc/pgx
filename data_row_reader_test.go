package pgx_test

import (
	"github.com/JackC/pgx"
	"testing"
)

func TestDataRowReaderReadValue(t *testing.T) {
	conn := getSharedConnection(t)

	test := func(sql string, expected interface{}) {
		var v interface{}

		onDataRow := func(r *pgx.DataRowReader) error {
			v = r.ReadValue()
			return nil
		}

		err := conn.SelectFunc(sql, onDataRow)
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if v != expected {
			t.Errorf("Expected: %#v Received: %#v", expected, v)
		}
	}

	test("select null", nil)
	test("select 'Jack'", "Jack")
	test("select true", true)
	test("select false", false)
	test("select 1::int2", int16(1))
	test("select 1::int4", int32(1))
	test("select 1::int8", int64(1))
	test("select 1.23::float4", float32(1.23))
	test("select 1.23::float8", float64(1.23))
}
