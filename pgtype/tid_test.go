package pgtype_test

import (
	"testing"

	"weavelab.xyz/pgx/pgtype"
	"weavelab.xyz/pgx/pgtype/testutil"
)

func TestTIDTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "tid", []interface{}{
		&pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Status: pgtype.Present},
		&pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Status: pgtype.Present},
		&pgtype.TID{Status: pgtype.Null},
	})
}
