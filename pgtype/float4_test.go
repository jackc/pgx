package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestFloat4Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float4", []pgxtest.ValueRoundTripTest{
		{Param: pgtype.Float4{Float32: -1, Valid: true}, Result: new(pgtype.Float4), Test: isExpectedEq(pgtype.Float4{Float32: -1, Valid: true})},
		{Param: pgtype.Float4{Float32: 0, Valid: true}, Result: new(pgtype.Float4), Test: isExpectedEq(pgtype.Float4{Float32: 0, Valid: true})},
		{Param: pgtype.Float4{Float32: 1, Valid: true}, Result: new(pgtype.Float4), Test: isExpectedEq(pgtype.Float4{Float32: 1, Valid: true})},
		{Param: float32(0.00001), Result: new(float32), Test: isExpectedEq(float32(0.00001))},
		{Param: float32(9999.99), Result: new(float32), Test: isExpectedEq(float32(9999.99))},
		{Param: pgtype.Float4{}, Result: new(pgtype.Float4), Test: isExpectedEq(pgtype.Float4{})},
		{Param: int64(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: "1.23", Result: new(string), Test: isExpectedEq("1.23")},
		{Param: nil, Result: new(*float32), Test: isExpectedEq((*float32)(nil))},
	})
}

func TestFloat4MarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Float4
		result string
	}{
		{source: pgtype.Float4{Float32: 0}, result: "null"},
		{source: pgtype.Float4{Float32: 1.23, Valid: true}, result: "1.23"},
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

func TestFloat4UnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Float4
	}{
		{source: "null", result: pgtype.Float4{Float32: 0}},
		{source: "1.23", result: pgtype.Float4{Float32: 1.23, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Float4
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
