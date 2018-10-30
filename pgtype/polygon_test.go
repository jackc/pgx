package pgtype_test

import (
	"testing"

	"github.com/weave-lab/pgx/pgtype"
	"github.com/weave-lab/pgx/pgtype/testutil"
)

func TestPolygonTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "polygon", []interface{}{
		&pgtype.Polygon{
			P:      []pgtype.Vec2{{3.14, 1.678901234}, {7.1, 5.234}, {5.0, 3.234}},
			Status: pgtype.Present,
		},
		&pgtype.Polygon{
			P:      []pgtype.Vec2{{3.14, -1.678}, {7.1, -5.234}, {23.1, 9.34}},
			Status: pgtype.Present,
		},
		&pgtype.Polygon{Status: pgtype.Null},
	})
}
