package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestBoxCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support box type")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "box", []pgxtest.ValueRoundTripTest{
		{
			Param: pgtype.Box{
				P:     [2]pgtype.Vec2{{7.1, 5.2345678}, {3.14, 1.678}},
				Valid: true,
			},
			Result: new(pgtype.Box),
			Test: isExpectedEq(pgtype.Box{
				P:     [2]pgtype.Vec2{{7.1, 5.2345678}, {3.14, 1.678}},
				Valid: true,
			}),
		},
		{
			Param: pgtype.Box{
				P:     [2]pgtype.Vec2{{7.1, 5.2345678}, {-13.14, -5.234}},
				Valid: true,
			},
			Result: new(pgtype.Box),
			Test: isExpectedEq(pgtype.Box{
				P:     [2]pgtype.Vec2{{7.1, 5.2345678}, {-13.14, -5.234}},
				Valid: true,
			}),
		},
		{Param: pgtype.Box{}, Result: new(pgtype.Box), Test: isExpectedEq(pgtype.Box{})},
		{Param: nil, Result: new(pgtype.Box), Test: isExpectedEq(pgtype.Box{})},
	})
}
