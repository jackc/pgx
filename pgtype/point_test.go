package pgtype_test

import (
	"testing"

	"github.com/weave-lab/pgx/pgtype"
	"github.com/weave-lab/pgx/pgtype/testutil"
)

func TestPointTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "point", []interface{}{
		&pgtype.Point{P: pgtype.Vec2{1.234, 5.6789012345}, Status: pgtype.Present},
		&pgtype.Point{P: pgtype.Vec2{-1.234, -5.6789}, Status: pgtype.Present},
		&pgtype.Point{Status: pgtype.Null},
	})
}
