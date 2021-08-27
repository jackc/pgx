package pgtype_test

import (
	"math/big"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestNumrangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "numrange", []interface{}{
		&pgtype.Numrange{
			LowerType: pgtype.Empty,
			UpperType: pgtype.Empty,
			Valid:     true,
		},
		&pgtype.Numrange{
			Lower:     pgtype.Numeric{Int: big.NewInt(-543), Exp: 3, Valid: true},
			Upper:     pgtype.Numeric{Int: big.NewInt(342), Exp: 1, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Numrange{
			Lower:     pgtype.Numeric{Int: big.NewInt(-42), Exp: 1, Valid: true},
			Upper:     pgtype.Numeric{Int: big.NewInt(-5), Exp: 0, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Numrange{
			Lower:     pgtype.Numeric{Int: big.NewInt(-42), Exp: 1, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Unbounded,
			Valid:     true,
		},
		&pgtype.Numrange{
			Upper:     pgtype.Numeric{Int: big.NewInt(-42), Exp: 1, Valid: true},
			LowerType: pgtype.Unbounded,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Numrange{},
	})
}
