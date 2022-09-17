package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestFloat8Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float8", []pgxtest.ValueRoundTripTest{
		{pgtype.Float8{Float64: -1, Valid: true}, new(pgtype.Float8), isExpectedEq(pgtype.Float8{Float64: -1, Valid: true})},
		{pgtype.Float8{Float64: 0, Valid: true}, new(pgtype.Float8), isExpectedEq(pgtype.Float8{Float64: 0, Valid: true})},
		{pgtype.Float8{Float64: 1, Valid: true}, new(pgtype.Float8), isExpectedEq(pgtype.Float8{Float64: 1, Valid: true})},
		{float64(0.00001), new(float64), isExpectedEq(float64(0.00001))},
		{float64(9999.99), new(float64), isExpectedEq(float64(9999.99))},
		{pgtype.Float8{}, new(pgtype.Float8), isExpectedEq(pgtype.Float8{})},
		{int64(1), new(int64), isExpectedEq(int64(1))},
		{"1.23", new(string), isExpectedEq("1.23")},
		{nil, new(*float64), isExpectedEq((*float64)(nil))},
	})
}
