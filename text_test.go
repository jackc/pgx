package pgtype_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestTextTranscode(t *testing.T) {
	for _, pgTypeName := range []string{"text", "varchar"} {
		testutil.TestSuccessfulTranscode(t, pgTypeName, []interface{}{
			&pgtype.Text{String: "", Status: pgtype.Present},
			&pgtype.Text{String: "foo", Status: pgtype.Present},
			&pgtype.Text{Status: pgtype.Null},
		})
	}
}

func TestTextSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Text
	}{
		{source: "foo", result: pgtype.Text{String: "foo", Status: pgtype.Present}},
		{source: _string("bar"), result: pgtype.Text{String: "bar", Status: pgtype.Present}},
		{source: (*string)(nil), result: pgtype.Text{Status: pgtype.Null}},
	}

	for i, tt := range successfulTests {
		var d pgtype.Text
		err := d.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if d != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, d)
		}
	}
}

func TestTextAssignTo(t *testing.T) {
	var s string
	var ps *string

	stringTests := []struct {
		src      pgtype.Text
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Text{String: "foo", Status: pgtype.Present}, dst: &s, expected: "foo"},
		{src: pgtype.Text{Status: pgtype.Null}, dst: &ps, expected: ((*string)(nil))},
	}

	for i, tt := range stringTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	var buf []byte

	bytesTests := []struct {
		src      pgtype.Text
		dst      *[]byte
		expected []byte
	}{
		{src: pgtype.Text{String: "foo", Status: pgtype.Present}, dst: &buf, expected: []byte("foo")},
		{src: pgtype.Text{Status: pgtype.Null}, dst: &buf, expected: nil},
	}

	for i, tt := range bytesTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if bytes.Compare(*tt.dst, tt.expected) != 0 {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, tt.dst)
		}
	}

	pointerAllocTests := []struct {
		src      pgtype.Text
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Text{String: "foo", Status: pgtype.Present}, dst: &ps, expected: "foo"},
	}

	for i, tt := range pointerAllocTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	errorTests := []struct {
		src pgtype.Text
		dst interface{}
	}{
		{src: pgtype.Text{Status: pgtype.Null}, dst: &s},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}

func TestTextMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Text
		result string
	}{
		{source: pgtype.Text{String: "", Status: pgtype.Null}, result: "null"},
		{source: pgtype.Text{String: "a", Status: pgtype.Present}, result: "\"a\""},
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
		{source: "null", result: pgtype.Text{String: "", Status: pgtype.Null}},
		{source: "\"a\"", result: pgtype.Text{String: "a", Status: pgtype.Present}},
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
