package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/pgtype/testutil"
)

func TestTidTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "tid", []interface{}{
		pgtype.Tid{BlockNumber: 42, OffsetNumber: 43, Status: pgtype.Present},
		pgtype.Tid{BlockNumber: 4294967295, OffsetNumber: 65535, Status: pgtype.Present},
		pgtype.Tid{Status: pgtype.Null},
	})
}
