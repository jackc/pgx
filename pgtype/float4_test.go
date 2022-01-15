package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestFloat4Codec(t *testing.T) {
	testPgxCodec(t, "float4", []PgxTranscodeTestCase{
		{pgtype.Float4{Float: -1, Valid: true}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{Float: -1, Valid: true})},
		{pgtype.Float4{Float: 0, Valid: true}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{Float: 0, Valid: true})},
		{pgtype.Float4{Float: 1, Valid: true}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{Float: 1, Valid: true})},
		{float32(0.00001), new(float32), isExpectedEq(float32(0.00001))},
		{float32(9999.99), new(float32), isExpectedEq(float32(9999.99))},
		{pgtype.Float4{}, new(pgtype.Float4), isExpectedEq(pgtype.Float4{})},
		{int64(1), new(int64), isExpectedEq(int64(1))},
		{nil, new(*float32), isExpectedEq((*float32)(nil))},
	})
}
