package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestInt8multirangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int8multirange", []interface{}{
		&pgtype.Int8multirange{
			Ranges: nil,
			Status: pgtype.Present,
		},
		&pgtype.Int8multirange{
			Ranges: []pgtype.Int8range{
				{
					Lower:     pgtype.Int8{Int: -543, Status: pgtype.Present},
					Upper:     pgtype.Int8{Int: 342, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
		&pgtype.Int8multirange{
			Ranges: []pgtype.Int8range{
				{
					Lower:     pgtype.Int8{Int: -42, Status: pgtype.Present},
					Upper:     pgtype.Int8{Int: -5, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
				{
					Lower:     pgtype.Int8{Int: 5, Status: pgtype.Present},
					Upper:     pgtype.Int8{Int: 42, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Status:    pgtype.Present,
				},
				{
					Lower:     pgtype.Int8{Int: 52, Status: pgtype.Present},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Unbounded,
					Status:    pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
	})
}

func TestInt8multirangeNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL: "select int8multirange(int8range(1, 14, '(]'), int8range(20, 25, '()'))",
			Value: pgtype.Int8multirange{
				Ranges: []pgtype.Int8range{
					{
						Lower:     pgtype.Int8{Int: 2, Status: pgtype.Present},
						Upper:     pgtype.Int8{Int: 15, Status: pgtype.Present},
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Status:    pgtype.Present,
					},
					{
						Lower:     pgtype.Int8{Int: 21, Status: pgtype.Present},
						Upper:     pgtype.Int8{Int: 25, Status: pgtype.Present},
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
