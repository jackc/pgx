package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestCircleTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support box type")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "circle", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true},
			Result: new(pgtype.Circle),
			Test:   isExpectedEq(pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true}),
		},
		{
			Param:  pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true},
			Result: new(pgtype.Circle),
			Test:   isExpectedEq(pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true}),
		},
		{Param: pgtype.Circle{}, Result: new(pgtype.Circle), Test: isExpectedEq(pgtype.Circle{})},
		{Param: nil, Result: new(pgtype.Circle), Test: isExpectedEq(pgtype.Circle{})},
	})
}
