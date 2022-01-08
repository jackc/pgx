package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestTextCodec(t *testing.T) {
	for _, pgTypeName := range []string{"text", "varchar"} {
		testPgxCodec(t, pgTypeName, []PgxTranscodeTestCase{
			{
				pgtype.Text{String: "", Valid: true},
				new(pgtype.Text),
				isExpectedEq(pgtype.Text{String: "", Valid: true}),
			},
			{
				pgtype.Text{String: "foo", Valid: true},
				new(pgtype.Text),
				isExpectedEq(pgtype.Text{String: "foo", Valid: true}),
			},
			{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
			{"foo", new(string), isExpectedEq("foo")},
			{rune('R'), new(rune), isExpectedEq(rune('R'))},
		})
	}
}

// name is PostgreSQL's special 63-byte data type, used for identifiers like table names.  The pg_class.relname column
// is a good example of where the name data type is used.
//
// TextCodec does not do length checking. Inputting a longer name into PostgreSQL will result in silent truncation to
// 63 bytes.
//
// Length checking would be possible with a Codec specialized for "name" but it would be perfect because a
// custom-compiled PostgreSQL could have set NAMEDATALEN to a different value rather than the default 63.
//
// So this is simply a smoke test of the name type.
func TestTextCodecName(t *testing.T) {
	testPgxCodec(t, "name", []PgxTranscodeTestCase{
		{
			pgtype.Text{String: "", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "", Valid: true}),
		},
		{
			pgtype.Text{String: "foo", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "foo", Valid: true}),
		},
		{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
		{"foo", new(string), isExpectedEq("foo")},
	})
}

// Test fixed length char types like char(3)
func TestTextCodecBPChar(t *testing.T) {
	testPgxCodec(t, "char(3)", []PgxTranscodeTestCase{
		{
			pgtype.Text{String: "a  ", Valid: true},
			new(pgtype.Text),
			isExpectedEq(pgtype.Text{String: "a  ", Valid: true}),
		},
		{nil, new(pgtype.Text), isExpectedEq(pgtype.Text{})},
		{"   ", new(string), isExpectedEq("   ")},
		{"", new(string), isExpectedEq("   ")},
		{" 嗨 ", new(string), isExpectedEq(" 嗨 ")},
	})
}

func TestTextMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Text
		result string
	}{
		{source: pgtype.Text{String: ""}, result: "null"},
		{source: pgtype.Text{String: "a", Valid: true}, result: "\"a\""},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestTextUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Text
	}{
		{source: "null", result: pgtype.Text{String: ""}},
		{source: "\"a\"", result: pgtype.Text{String: "a", Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Text
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
