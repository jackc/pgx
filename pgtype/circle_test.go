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
			pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true},
			new(pgtype.Circle),
			isExpectedEq(pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true}),
		},
		{
			pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true},
			new(pgtype.Circle),
			isExpectedEq(pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true}),
		},
		{pgtype.Circle{}, new(pgtype.Circle), isExpectedEq(pgtype.Circle{})},
		{nil, new(pgtype.Circle), isExpectedEq(pgtype.Circle{})},
	})
}
