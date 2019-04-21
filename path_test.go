package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestPathTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "path", []interface{}{
		&pgtype.Path{
			P:      []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}},
			Closed: false,
			Status: pgtype.Present,
		},
		&pgtype.Path{
			P:      []pgtype.Vec2{{3.14, 1.678}, {7.1, 5.234}, {23.1, 9.34}},
			Closed: true,
			Status: pgtype.Present,
		},
		&pgtype.Path{
			P:      []pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
			Closed: true,
			Status: pgtype.Present,
		},
		&pgtype.Path{Status: pgtype.Null},
	})
}
