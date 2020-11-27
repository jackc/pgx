package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
)

func TestParseUntypedTextArray(t *testing.T) {
	tests := []struct {
		source string
		result pgtype.UntypedTextArray
	}{
		{
			source: "{}",
			result: pgtype.UntypedTextArray{
				Elements:   nil,
				Quoted:     nil,
				Dimensions: nil,
			},
		},
		{
			source: "{1}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{"1"},
				Quoted:     []bool{false},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{a,b}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{"a", "b"},
				Quoted:     []bool{false, false},
				Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			},
		},
		{
			source: `{"NULL"}`,
			result: pgtype.UntypedTextArray{
				Elements:   []string{"NULL"},
				Quoted:     []bool{true},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{""}`,
			result: pgtype.UntypedTextArray{
				Elements:   []string{""},
				Quoted:     []bool{true},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: `{"He said, \"Hello.\""}`,
			result: pgtype.UntypedTextArray{
				Elements:   []string{`He said, "Hello."`},
				Quoted:     []bool{true},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 1}},
			},
		},
		{
			source: "{{a,b},{c,d},{e,f}}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{"a", "b", "c", "d", "e", "f"},
				Quoted:     []bool{false, false, false, false, false, false},
				Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			},
		},
		{
			source: "{{{a,b},{c,d},{e,f}},{{a,b},{c,d},{e,f}}}",
			result: pgtype.UntypedTextArray{
				Elements: []string{"a", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f"},
				Quoted:   []bool{false, false, false, false, false, false, false, false, false, false, false, false},
				Dimensions: []pgtype.ArrayDimension{
					{Length: 2, LowerBound: 1},
					{Length: 3, LowerBound: 1},
					{Length: 2, LowerBound: 1},
				},
			},
		},
		{
			source: "[4:4]={1}",
			result: pgtype.UntypedTextArray{
				Elements:   []string{"1"},
				Quoted:     []bool{false},
				Dimensions: []pgtype.ArrayDimension{{Length: 1, LowerBound: 4}},
			},
		},
		{
			source: "[4:5][2:3]={{a,b},{c,d}}",
			result: pgtype.UntypedTextArray{
				Elements: []string{"a", "b", "c", "d"},
				Quoted:   []bool{false, false, false, false},
				Dimensions: []pgtype.ArrayDimension{
					{Length: 2, LowerBound: 4},
					{Length: 2, LowerBound: 2},
				},
			},
		},
	}

	for i, tt := range tests {
		r, err := pgtype.ParseUntypedTextArray(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		if !reflect.DeepEqual(*r, tt.result) {
			t.Errorf("%d: expected %+v to be parsed to %+v, but it was %+v", i, tt.source, tt.result, *r)
		}
	}
}

// https://github.com/jackc/pgx/issues/881
func TestArrayAssignToEmptyToNonSlice(t *testing.T) {
	var a pgtype.Int4Array
	err := a.Set([]int32{})
	require.NoError(t, err)

	var iface interface{}
	err = a.AssignTo(&iface)
	require.EqualError(t, err, "cannot assign *pgtype.Int4Array to *interface {}")
}
