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

func TestNullHstoreTranscode(t *testing.T) {
	t.Parallel()

	conn := mustConnect(t, *defaultConnConfig)
	defer closeConn(t, conn)

	type test struct {
		nullHstore  pgx.NullHstore
		description string
	}

	tests := []test{
		{pgx.NullHstore{}, "null"},
		{pgx.NullHstore{Valid: true}, "empty"},
		{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: "bar", Valid: true}},
			Valid:  true},
			"single key/value"},
		{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: "bar", Valid: true}, "baz": pgx.NullString{String: "quz", Valid: true}},
			Valid:  true},
			"multiple key/values"},
		{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"NULL": pgx.NullString{String: "bar", Valid: true}},
			Valid:  true},
			`string "NULL" key`},
		{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: "NULL", Valid: true}},
			Valid:  true},
			`string "NULL" value`},
		{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: "", Valid: false}},
			Valid:  true},
			`NULL value`},
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
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{sst.input + "foo": pgx.NullString{String: "bar", Valid: true}},
			Valid:  true},
			"key with " + sst.description + " at beginning"})
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo" + sst.input + "foo": pgx.NullString{String: "bar", Valid: true}},
			Valid:  true},
			"key with " + sst.description + " in middle"})
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo" + sst.input: pgx.NullString{String: "bar", Valid: true}},
			Valid:  true},
			"key with " + sst.description + " at end"})
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{sst.input: pgx.NullString{String: "bar", Valid: true}},
			Valid:  true},
			"key is " + sst.description})

		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: sst.input + "bar", Valid: true}},
			Valid:  true},
			"value with " + sst.description + " at beginning"})
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: "bar" + sst.input + "bar", Valid: true}},
			Valid:  true},
			"value with " + sst.description + " in middle"})
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: "bar" + sst.input, Valid: true}},
			Valid:  true},
			"value with " + sst.description + " at end"})
		tests = append(tests, test{pgx.NullHstore{
			Hstore: map[string]pgx.NullString{"foo": pgx.NullString{String: sst.input, Valid: true}},
			Valid:  true},
			"value is " + sst.description})
	}

	for _, tt := range tests {
		var result pgx.NullHstore
		err := conn.QueryRow("select $1::hstore", tt.nullHstore).Scan(&result)
		if err != nil {
			t.Errorf(`%s: QueryRow.Scan returned an error: %v`, tt.description, err)
		}

		if result.Valid != tt.nullHstore.Valid {
			t.Errorf(`%s: Valid mismatch - expected %v, received %v`, tt.description, tt.nullHstore.Valid, result.Valid)
		}

		for key, inValue := range tt.nullHstore.Hstore {
			outValue, ok := result.Hstore[key]
			if ok {
				if inValue != outValue {
					t.Errorf(`%s: Key %s mismatch - expected %v, received %v`, tt.description, key, inValue, outValue)
				}
			} else {
				t.Errorf(`%s: Missing key %s`, tt.description, key)
			}
		}

		ensureConnValid(t, conn)
	}
}
