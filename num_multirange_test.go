package pgtype_test

import (
	"math/big"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestNumericMultirangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "nummultirange", []interface{}{
		&pgtype.Nummultirange{
			Ranges: nil,
			Status: pgtype.Present,
		},
		&pgtype.Nummultirange{
			Ranges: []pgtype.Numrange{
				{
					Lower:     pgtype.Numeric{Int: big.NewInt(-543), Exp: 3, Status: pgtype.Present},
					Upper:     pgtype.Numeric{Int: big.NewInt(342), Exp: 1, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
		&pgtype.Nummultirange{
			Ranges: []pgtype.Numrange{
				{
					Lower:     pgtype.Numeric{Int: big.NewInt(-42), Exp: 1, Status: pgtype.Present},
					Upper:     pgtype.Numeric{Int: big.NewInt(-5), Exp: 0, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
				{
					Lower:     pgtype.Numeric{Int: big.NewInt(5), Exp: 1, Status: pgtype.Present},
					Upper:     pgtype.Numeric{Int: big.NewInt(42), Exp: 1, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Inclusive,
					Status:    pgtype.Present,
				},
				{
					Lower:     pgtype.Numeric{Int: big.NewInt(42), Exp: 2, Status: pgtype.Present},
					LowerType: pgtype.Exclusive,
					UpperType: pgtype.Unbounded,
					Status:    pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
	})
}
