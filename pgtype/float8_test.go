package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestFloat8Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float8", []pgxtest.ValueRoundTripTest{
		{Param: pgtype.Float8{Float64: -1, Valid: true}, Result: new(pgtype.Float8), Test: isExpectedEq(pgtype.Float8{Float64: -1, Valid: true})},
		{Param: pgtype.Float8{Float64: 0, Valid: true}, Result: new(pgtype.Float8), Test: isExpectedEq(pgtype.Float8{Float64: 0, Valid: true})},
		{Param: pgtype.Float8{Float64: 1, Valid: true}, Result: new(pgtype.Float8), Test: isExpectedEq(pgtype.Float8{Float64: 1, Valid: true})},
		{Param: float64(0.00001), Result: new(float64), Test: isExpectedEq(float64(0.00001))},
		{Param: float64(9999.99), Result: new(float64), Test: isExpectedEq(float64(9999.99))},
		{Param: pgtype.Float8{}, Result: new(pgtype.Float8), Test: isExpectedEq(pgtype.Float8{})},
		{Param: int64(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: "1.23", Result: new(string), Test: isExpectedEq("1.23")},
		{Param: nil, Result: new(*float64), Test: isExpectedEq((*float64)(nil))},
	})
}

func TestFloat8MarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Float8
		result string
	}{
		{source: pgtype.Float8{Float64: 0}, result: "null"},
		{source: pgtype.Float8{Float64: 1.23, Valid: true}, result: "1.23"},
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

func TestFloat8UnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Float8
	}{
		{source: "null", result: pgtype.Float8{Float64: 0}},
		{source: "1.23", result: pgtype.Float8{Float64: 1.23, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Float8
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
