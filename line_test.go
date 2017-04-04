package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestLineTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "line", []interface{}{
		&pgtype.Line{
			A: 1.23, B: 4.56, C: 7.89,
			Status: pgtype.Present,
		},
		&pgtype.Line{
			A: -1.23, B: -4.56, C: -7.89,
			Status: pgtype.Present,
		},
		&pgtype.Line{Status: pgtype.Null},
	})
}
