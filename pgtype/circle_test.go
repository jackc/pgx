package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestCircleTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "circle", []interface{}{
		&pgtype.Circle{P: pgtype.Vec2{1.234, 5.67890123}, R: 3.5, Valid: true},
		&pgtype.Circle{P: pgtype.Vec2{-1.234, -5.6789}, R: 12.9, Valid: true},
		&pgtype.Circle{},
	})
}
