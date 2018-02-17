package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/pgtype/testutil"
)

func TestLineTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	if _, ok := conn.ConnInfo.DataTypeForName("line"); !ok {
		t.Skip("Skipping due to no line type")
	}

	testutil.TestSuccessfulTranscode(t, "line", []interface{}{
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
