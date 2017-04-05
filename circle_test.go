package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestCircleTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "circle", []interface{}{
		&pgtype.Circle{P: pgtype.Vec2{1.234, 5.6789}, R: 3.5, Status: pgtype.Present},
		&pgtype.Circle{P: pgtype.Vec2{-1.234, -5.6789}, R: 12.9, Status: pgtype.Present},
		&pgtype.Circle{Status: pgtype.Null},
	})
}
