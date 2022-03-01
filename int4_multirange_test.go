package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestInt4multirangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int4multirange", []interface{}{
		&pgtype.Int4multirange{
			Ranges: nil,
			Status: pgtype.Present,
		},
		&pgtype.Int4multirange{
			Ranges: []pgtype.Int4range{
				{
					Lower:     pgtype.Int4{Int: -543, Status: pgtype.Present},
					Upper:     pgtype.Int4{Int: 342, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
		&pgtype.Int4multirange{
			Ranges: []pgtype.Int4range{
				{
					Lower:     pgtype.Int4{Int: -42, Status: pgtype.Present},
					Upper:     pgtype.Int4{Int: -5, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
				{
					Lower:     pgtype.Int4{Int: 5, Status: pgtype.Present},
					Upper:     pgtype.Int4{Int: 42, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
				{
					Lower:     pgtype.Int4{Int: 52, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Unbounded,
					Status:    pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
	})
}

func TestInt4multirangeNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL: "select int4multirange(int4range(1, 14, '(]'), int4range(20, 25, '()'))",
			Value: pgtype.Int4multirange{
				Ranges: []pgtype.Int4range{
					{
						Lower:     pgtype.Int4{Int: 2, Status: pgtype.Present},
						Upper:     pgtype.Int4{Int: 15, Status: pgtype.Present},
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Status:    pgtype.Present,
					},
					{
						Lower:     pgtype.Int4{Int: 21, Status: pgtype.Present},
						Upper:     pgtype.Int4{Int: 25, Status: pgtype.Present},
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Status:    pgtype.Present,
					},
				},
				Status: pgtype.Present,
			},
		},
	})
}
