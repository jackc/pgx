package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestInt4rangeTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "int4range", []interface{}{
		pgtype.Int4range{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Status: pgtype.Present},
		pgtype.Int4range{Lower: pgtype.Int4{Int: 1, Status: pgtype.Present}, Upper: pgtype.Int4{Int: 10, Status: pgtype.Present}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Status: pgtype.Present},
		pgtype.Int4range{Lower: pgtype.Int4{Int: -42, Status: pgtype.Present}, Upper: pgtype.Int4{Int: -5, Status: pgtype.Present}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Status: pgtype.Present},
		pgtype.Int4range{Status: pgtype.Null},
	})
}

func TestInt4rangeNormalize(t *testing.T) {
	testSuccessfulNormalize(t, []normalizeTest{
		{
			sql:   "select int4range(1, 10, '(]')",
			value: pgtype.Int4range{Lower: pgtype.Int4{Int: 2, Status: pgtype.Present}, Upper: pgtype.Int4{Int: 11, Status: pgtype.Present}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Status: pgtype.Present},
		},
	})
}
