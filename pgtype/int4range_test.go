package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestInt4rangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int4range", []interface{}{
		&pgtype.Int4range{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
		&pgtype.Int4range{Lower: pgtype.Int4{Int: 1, Valid: true}, Upper: pgtype.Int4{Int: 10, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		&pgtype.Int4range{Lower: pgtype.Int4{Int: -42, Valid: true}, Upper: pgtype.Int4{Int: -5, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		&pgtype.Int4range{Lower: pgtype.Int4{Int: 1, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Unbounded, Valid: true},
		&pgtype.Int4range{Upper: pgtype.Int4{Int: 1, Valid: true}, LowerType: pgtype.Unbounded, UpperType: pgtype.Exclusive, Valid: true},
		&pgtype.Int4range{},
	})
}

func TestInt4rangeNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL:   "select int4range(1, 10, '(]')",
			Value: pgtype.Int4range{Lower: pgtype.Int4{Int: 2, Valid: true}, Upper: pgtype.Int4{Int: 11, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		},
	})
}
