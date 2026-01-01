package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestLsegTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support type lseg")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "lseg", []pgxtest.ValueRoundTripTest{
		{
			Param: pgtype.Lseg{
				P:     [2]pgtype.Vec2{{3.14, 1.678}, {7.1, 5.2345678901}},
				Valid: true,
			},
			Result: new(pgtype.Lseg),
			Test: isExpectedEq(pgtype.Lseg{
				P:     [2]pgtype.Vec2{{3.14, 1.678}, {7.1, 5.2345678901}},
				Valid: true,
			}),
		},
		{
			Param: pgtype.Lseg{
				P:     [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
				Valid: true,
			},
			Result: new(pgtype.Lseg),
			Test: isExpectedEq(pgtype.Lseg{
				P:     [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
				Valid: true,
			}),
		},
		{Param: pgtype.Lseg{}, Result: new(pgtype.Lseg), Test: isExpectedEq(pgtype.Lseg{})},
		{Param: nil, Result: new(pgtype.Lseg), Test: isExpectedEq(pgtype.Lseg{})},
	})
}
