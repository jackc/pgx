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
