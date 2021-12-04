package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestBPCharArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "char(8)[]", []interface{}{
		&pgtype.BPCharArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.BPCharArray{
			Elements: []pgtype.BPChar{
				pgtype.BPChar{String: "foo     ", Valid: true},
				pgtype.BPChar{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.BPCharArray{},
		&pgtype.BPCharArray{
			Elements: []pgtype.BPChar{
				pgtype.BPChar{String: "bar     ", Valid: true},
				pgtype.BPChar{String: "NuLL    ", Valid: true},
				pgtype.BPChar{String: `wow"quz\`, Valid: true},
				pgtype.BPChar{String: "1       ", Valid: true},
				pgtype.BPChar{String: "1       ", Valid: true},
				pgtype.BPChar{String: "null    ", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 3, LowerBound: 1},
				{Length: 2, LowerBound: 1},
			},
			Valid: true,
		},
		&pgtype.BPCharArray{
			Elements: []pgtype.BPChar{
				pgtype.BPChar{String: " bar    ", Valid: true},
				pgtype.BPChar{String: "    baz ", Valid: true},
				pgtype.BPChar{String: "    quz ", Valid: true},
				pgtype.BPChar{String: "foo     ", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}
