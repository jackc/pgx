package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestInt8rangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "Int8range", []interface{}{
		&pgtype.Int8range{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
		&pgtype.Int8range{Lower: pgtype.Int8{Int: 1, Valid: true}, Upper: pgtype.Int8{Int: 10, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		&pgtype.Int8range{Lower: pgtype.Int8{Int: -42, Valid: true}, Upper: pgtype.Int8{Int: -5, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		&pgtype.Int8range{Lower: pgtype.Int8{Int: 1, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Unbounded, Valid: true},
		&pgtype.Int8range{Upper: pgtype.Int8{Int: 1, Valid: true}, LowerType: pgtype.Unbounded, UpperType: pgtype.Exclusive, Valid: true},
		&pgtype.Int8range{},
	})
}

func TestInt8rangeNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL:   "select Int8range(1, 10, '(]')",
			Value: pgtype.Int8range{Lower: pgtype.Int8{Int: 2, Valid: true}, Upper: pgtype.Int8{Int: 11, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		},
	})
}
