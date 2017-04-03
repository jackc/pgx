package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestPointTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "point", []interface{}{
		&pgtype.Point{X: 1.234, Y: 5.6789, Status: pgtype.Present},
		&pgtype.Point{X: -1.234, Y: -5.6789, Status: pgtype.Present},
		&pgtype.Point{Status: pgtype.Null},
	})
}
