package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestFloat4Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float4", []pgxtest.ValueRoundTripTest{
		{pgtype.Float4{Float32: -1, Valid: true}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{Float32: -1, Valid: true})},
		{pgtype.Float4{Float32: 0, Valid: true}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{Float32: 0, Valid: true})},
		{pgtype.Float4{Float32: 1, Valid: true}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{Float32: 1, Valid: true})},
		{float32(0.00001), new(float32), isExpectedEq(float32(0.00001))},
		{float32(9999.99), new(float32), isExpectedEq(float32(9999.99))},
		{pgtype.Float4{}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{})},
		{int64(1), new(int64), isExpectedEq(int64(1))},
		{"1.23", new(string), isExpectedEq("1.23")},
		{nil, new(*float32), isExpectedEq((*float32)(nil))},
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
