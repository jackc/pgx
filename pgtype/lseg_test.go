package pgtype_test

import (
	"testing"

	"github.com/weave-lab/pgx/pgtype"
	"github.com/weave-lab/pgx/pgtype/testutil"
)

func TestLsegTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "lseg", []interface{}{
		&pgtype.Lseg{
			P:      [2]pgtype.Vec2{{3.14, 1.678}, {7.1, 5.2345678901}},
			Status: pgtype.Present,
		},
		&pgtype.Lseg{
			P:      [2]pgtype.Vec2{{7.1, 1.678}, {-13.14, -5.234}},
			Status: pgtype.Present,
		},
		&pgtype.Lseg{Status: pgtype.Null},
	})
}
