package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestCircleTranscode(t *testing.T) {
	testPgxCodec(t, "circle", []PgxTranscodeTestCase{
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
