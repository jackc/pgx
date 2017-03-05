package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestTextTranscode(t *testing.T) {
	for _, pgTypeName := range []string{"text", "varchar"} {
		testSuccessfulTranscode(t, pgTypeName, []interface{}{
			pgtype.Text{String: "", Status: pgtype.Present},
			pgtype.Text{String: "foo", Status: pgtype.Present},
			pgtype.Text{Status: pgtype.Null},
		})
	}
}

func TestTextConvertFrom(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Text
	}{
		{source: pgtype.Text{String: "foo", Status: pgtype.Present}, result: pgtype.Text{String: "foo", Status: pgtype.Present}},
		{source: "foo", result: pgtype.Text{String: "foo", Status: pgtype.Present}},
		{source: _string("bar"), result: pgtype.Text{String: "bar", Status: pgtype.Present}},
		{source: (*string)(nil), result: pgtype.Text{Status: pgtype.Null}},
	}

	for i, tt := range successfulTests {
		var d pgtype.Text
		err := d.ConvertFrom(tt.source)
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

	simpleTests := []struct {
		src      pgtype.Text
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Text{String: "foo", Status: pgtype.Present}, dst: &s, expected: "foo"},
		{src: pgtype.Text{Status: pgtype.Null}, dst: &ps, expected: ((*string)(nil))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
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
