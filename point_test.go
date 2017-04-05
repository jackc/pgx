package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestPointTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "point", []interface{}{
		&pgtype.Point{P: pgtype.Vec2{1.234, 5.6789}, Status: pgtype.Present},
		&pgtype.Point{P: pgtype.Vec2{-1.234, -5.6789}, Status: pgtype.Present},
		&pgtype.Point{Status: pgtype.Null},
	})
}
