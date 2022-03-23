package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestCircleTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support box type")

	testutil.RunTranscodeTests(t, "circle", []testutil.TranscodeTestCase{
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
