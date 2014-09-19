package pgx_test

import (
	"github.com/jackc/pgx"
	"testing"
)

func TestHstoreTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type test struct {
		hstore      pgx.Hstore
		description string
	}

	tests := []test{
		{pgx.Hstore{}, "empty"},
		{pgx.Hstore{"foo": "bar"}, "single key/value"},
		{pgx.Hstore{"foo": "bar", "baz": "quz"}, "multiple key/values"},
		{pgx.Hstore{"NULL": "bar"}, `string "NULL" key`},
		{pgx.Hstore{"foo": "NULL"}, `string "NULL" value`},
	}

	specialStringTests := []struct {
		input       string
		description string
	}{
		{`"`, `double quote (")`},
		{`'`, `single quote (')`},
		{`\`, `backslash (\)`},
		{`\\`, `multiple backslashes (\\)`},
		{`=>`, `separator (=>)`},
		{` `, `space`},
		{`\ / / \\ => " ' " '`, `multiple special characters`},
	}
	for _, sst := range specialStringTests {
		tests = append(tests, test{pgx.Hstore{sst.input + "foo": "bar"}, "key with " + sst.description + " at beginning"})
		tests = append(tests, test{pgx.Hstore{"foo" + sst.input + "foo": "bar"}, "key with " + sst.description + " in middle"})
		tests = append(tests, test{pgx.Hstore{"foo" + sst.input: "bar"}, "key with " + sst.description + " at end"})
		tests = append(tests, test{pgx.Hstore{sst.input: "bar"}, "key is " + sst.description})

		tests = append(tests, test{pgx.Hstore{"foo": sst.input + "bar"}, "value with " + sst.description + " at beginning"})
		tests = append(tests, test{pgx.Hstore{"foo": "bar" + sst.input + "bar"}, "value with " + sst.description + " in middle"})
		tests = append(tests, test{pgx.Hstore{"foo": "bar" + sst.input}, "value with " + sst.description + " at end"})
		tests = append(tests, test{pgx.Hstore{"foo": sst.input}, "value is " + sst.description})
	}

	for _, tt := range tests {
		var result pgx.Hstore
		err := conn.QueryRow("select $1::hstore", tt.hstore).Scan(&result)
		if err != nil {
			t.Errorf(`%s: QueryRow.Scan returned an error: %v`, tt.description, err)
		}

		for key, inValue := range tt.hstore {
			outValue, ok := result[key]
			if ok {
				if inValue != outValue {
					t.Errorf(`%s: Key %s mismatch - expected %s, received %s`, tt.description, key, inValue, outValue)
				}
			} else {
				t.Errorf(`%s: Missing key %s`, tt.description, key)
			}
		}

		ensureConnValid(t, conn)
	}
}
