package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestHstoreTranscode(t *testing.T) {
	text := func(s string) pgtype.Text {
		return pgtype.Text{String: s, Valid: true}
	}

	values := []interface{}{
		&pgtype.Hstore{Map: map[string]pgtype.Text{}, Valid: true},
		&pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text(""), "bar": text(""), "baz": text("123")}, Valid: true},
		&pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("bar")}, Valid: true},
		&pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("bar"), "baz": text("quz")}, Valid: true},
		&pgtype.Hstore{Map: map[string]pgtype.Text{"NULL": text("bar")}, Valid: true},
		&pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("NULL")}, Valid: true},
		&pgtype.Hstore{Map: map[string]pgtype.Text{"": text("bar")}, Valid: true},
		&pgtype.Hstore{
			Map:   map[string]pgtype.Text{"a": text("a"), "b": {}, "c": text("c"), "d": {}, "e": text("e")},
			Valid: true,
		},
		&pgtype.Hstore{},
	}

	specialStrings := []string{
		`"`,
		`'`,
		`\`,
		`\\`,
		`=>`,
		` `,
		`\ / / \\ => " ' " '`,
	}
	for _, s := range specialStrings {
		// Special key values
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{s + "foo": text("bar")}, Valid: true})         // at beginning
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo" + s + "bar": text("bar")}, Valid: true}) // in middle
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo" + s: text("bar")}, Valid: true})         // at end
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{s: text("bar")}, Valid: true})                 // is key

		// Special value values
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text(s + "bar")}, Valid: true})         // at beginning
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("foo" + s + "bar")}, Valid: true}) // in middle
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("foo" + s)}, Valid: true})         // at end
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text(s)}, Valid: true})                 // is key
	}

	testutil.TestSuccessfulTranscodeEqFunc(t, "hstore", values, func(ai, bi interface{}) bool {
		a := ai.(pgtype.Hstore)
		b := bi.(pgtype.Hstore)

		if len(a.Map) != len(b.Map) || a.Valid != b.Valid {
			return false
		}

		for k := range a.Map {
			if a.Map[k] != b.Map[k] {
				return false
			}
		}

		return true
	})
}

func TestHstoreTranscodeNullable(t *testing.T) {
	text := func(s string, valid bool) pgtype.Text {
		return pgtype.Text{String: s, Valid: valid}
	}

	values := []interface{}{
		&pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("", false)}, Valid: true},
	}

	specialStrings := []string{
		`"`,
		`'`,
		`\`,
		`\\`,
		`=>`,
		` `,
		`\ / / \\ => " ' " '`,
	}
	for _, s := range specialStrings {
		// Special key values
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{s + "foo": text("", false)}, Valid: true})         // at beginning
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo" + s + "bar": text("", false)}, Valid: true}) // in middle
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{"foo" + s: text("", false)}, Valid: true})         // at end
		values = append(values, &pgtype.Hstore{Map: map[string]pgtype.Text{s: text("", false)}, Valid: true})                 // is key
	}

	testutil.TestSuccessfulTranscodeEqFunc(t, "hstore", values, func(ai, bi interface{}) bool {
		a := ai.(pgtype.Hstore)
		b := bi.(pgtype.Hstore)

		if len(a.Map) != len(b.Map) || a.Valid != b.Valid {
			return false
		}

		for k := range a.Map {
			if a.Map[k] != b.Map[k] {
				return false
			}
		}

		return true
	})
}

func TestHstoreSet(t *testing.T) {
	successfulTests := []struct {
		src    map[string]string
		result pgtype.Hstore
	}{
		{src: map[string]string{"foo": "bar"}, result: pgtype.Hstore{Map: map[string]pgtype.Text{"foo": {String: "bar", Valid: true}}, Valid: true}},
	}

	for i, tt := range successfulTests {
		var dst pgtype.Hstore
		err := dst.Set(tt.src)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(dst, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.src, tt.result, dst)
		}
	}
}

func TestHstoreSetNullable(t *testing.T) {
	successfulTests := []struct {
		src    map[string]*string
		result pgtype.Hstore
	}{
		{src: map[string]*string{"foo": nil}, result: pgtype.Hstore{Map: map[string]pgtype.Text{"foo": {}}, Valid: true}},
	}

	for i, tt := range successfulTests {
		var dst pgtype.Hstore
		err := dst.Set(tt.src)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(dst, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.src, tt.result, dst)
		}
	}
}

func TestHstoreAssignTo(t *testing.T) {
	var m map[string]string

	simpleTests := []struct {
		src      pgtype.Hstore
		dst      *map[string]string
		expected map[string]string
	}{
		{src: pgtype.Hstore{Map: map[string]pgtype.Text{"foo": {String: "bar", Valid: true}}, Valid: true}, dst: &m, expected: map[string]string{"foo": "bar"}},
		{src: pgtype.Hstore{}, dst: &m, expected: ((map[string]string)(nil))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(*tt.dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, *tt.dst)
		}
	}
}

func TestHstoreAssignToNullable(t *testing.T) {
	var m map[string]*string

	simpleTests := []struct {
		src      pgtype.Hstore
		dst      *map[string]*string
		expected map[string]*string
	}{
		{src: pgtype.Hstore{Map: map[string]pgtype.Text{"foo": {}}, Valid: true}, dst: &m, expected: map[string]*string{"foo": nil}},
		{src: pgtype.Hstore{}, dst: &m, expected: ((map[string]*string)(nil))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(*tt.dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, *tt.dst)
		}
	}
}
