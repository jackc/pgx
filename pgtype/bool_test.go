package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestBoolCodec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "bool", []pgxtest.ValueRoundTripTest{
		{Param: true, Result: new(bool), Test: isExpectedEq(true)},
		{Param: false, Result: new(bool), Test: isExpectedEq(false)},
		{Param: true, Result: new(pgtype.Bool), Test: isExpectedEq(pgtype.Bool{Bool: true, Valid: true})},
		{Param: pgtype.Bool{}, Result: new(pgtype.Bool), Test: isExpectedEq(pgtype.Bool{})},
		{Param: nil, Result: new(*bool), Test: isExpectedEq((*bool)(nil))},
	})
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
