package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestBoolTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "bool", []interface{}{
		&pgtype.Bool{Bool: false, Valid: true},
		&pgtype.Bool{Bool: true, Valid: true},
		&pgtype.Bool{Bool: false},
	})
}

func TestBoolSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Bool
	}{
		{source: true, result: pgtype.Bool{Bool: true, Valid: true}},
		{source: false, result: pgtype.Bool{Bool: false, Valid: true}},
		{source: "true", result: pgtype.Bool{Bool: true, Valid: true}},
		{source: "false", result: pgtype.Bool{Bool: false, Valid: true}},
		{source: "t", result: pgtype.Bool{Bool: true, Valid: true}},
		{source: "f", result: pgtype.Bool{Bool: false, Valid: true}},
		{source: _bool(true), result: pgtype.Bool{Bool: true, Valid: true}},
		{source: _bool(false), result: pgtype.Bool{Bool: false, Valid: true}},
		{source: nil, result: pgtype.Bool{}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Bool
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestBoolAssignTo(t *testing.T) {
	var b bool
	var _b _bool
	var pb *bool
	var _pb *_bool

	simpleTests := []struct {
		src      pgtype.Bool
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Bool{Bool: false, Valid: true}, dst: &b, expected: false},
		{src: pgtype.Bool{Bool: true, Valid: true}, dst: &b, expected: true},
		{src: pgtype.Bool{Bool: false, Valid: true}, dst: &_b, expected: _bool(false)},
		{src: pgtype.Bool{Bool: true, Valid: true}, dst: &_b, expected: _bool(true)},
		{src: pgtype.Bool{Bool: false}, dst: &pb, expected: ((*bool)(nil))},
		{src: pgtype.Bool{Bool: false}, dst: &_pb, expected: ((*_bool)(nil))},
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
		src      pgtype.Bool
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Bool{Bool: true, Valid: true}, dst: &pb, expected: true},
		{src: pgtype.Bool{Bool: true, Valid: true}, dst: &_pb, expected: _bool(true)},
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
}

func TestBoolMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Bool
		result string
	}{
		{source: pgtype.Bool{}, result: "null"},
		{source: pgtype.Bool{Bool: true, Valid: true}, result: "true"},
		{source: pgtype.Bool{Bool: false, Valid: true}, result: "false"},
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

func TestBoolUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Bool
	}{
		{source: "null", result: pgtype.Bool{}},
		{source: "true", result: pgtype.Bool{Bool: true, Valid: true}},
		{source: "false", result: pgtype.Bool{Bool: false, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Bool
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
