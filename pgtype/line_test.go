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

	// line may exist but not be usable on 9.3 :(
	var isPG93 bool
	err := conn.QueryRow("select version() ~ '9.3'").Scan(&isPG93)
	if err != nil {
		t.Fatal(err)
	}
	if isPG93 {
		t.Skip("Skipping due to unimplemented line type in PG 9.3")
	}

	testutil.TestSuccessfulTranscode(t, "line", []interface{}{
		&pgtype.Line{
			A: 1.23, B: 4.56, C: 7.89012345,
			Status: pgtype.Present,
		},
		&pgtype.Line{
			A: -1.23, B: -4.56, C: -7.89,
			Status: pgtype.Present,
		},
		&pgtype.Line{Status: pgtype.Null},
	})
}
