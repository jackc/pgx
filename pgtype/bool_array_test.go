package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestBoolArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "bool[]", []interface{}{
		&pgtype.BoolArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.BoolArray{
			Elements: []pgtype.Bool{
				{Bool: true, Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.BoolArray{},
		&pgtype.BoolArray{
			Elements: []pgtype.Bool{
				{Bool: true, Valid: true},
				{Bool: true, Valid: true},
				{Bool: false, Valid: true},
				{Bool: true, Valid: true},
				{},
				{Bool: false, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.BoolArray{
			Elements: []pgtype.Bool{
				{Bool: true, Valid: true},
				{Bool: false, Valid: true},
				{Bool: true, Valid: true},
				{Bool: false, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestBoolArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.BoolArray
	}{
		{
			source: []bool{true},
			result: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]bool)(nil)),
			result: pgtype.BoolArray{},
		},
		{
			source: [][]bool{{true}, {false}},
			result: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]bool{{{{true, false, true}}}, {{{false, true, false}}}},
			result: pgtype.BoolArray{
				Elements: []pgtype.Bool{
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1]bool{{true}, {false}},
			result: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]bool{{{{true, false, true}}}, {{{false, true, false}}}},
			result: pgtype.BoolArray{
				Elements: []pgtype.Bool{
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.BoolArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestBoolArrayAssignTo(t *testing.T) {
	var boolSlice []bool
	type _boolSlice []bool
	var namedBoolSlice _boolSlice
	var boolSliceDim2 [][]bool
	var boolSliceDim4 [][][][]bool
	var boolArrayDim2 [2][1]bool
	var boolArrayDim4 [2][1][1][3]bool

	simpleTests := []struct {
		src      pgtype.BoolArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &boolSlice,
			expected: []bool{true},
		},
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedBoolSlice,
			expected: _boolSlice{true},
		},
		{
			src:      pgtype.BoolArray{},
			dst:      &boolSlice,
			expected: (([]bool)(nil)),
		},
		{
			src:      pgtype.BoolArray{Valid: true},
			dst:      &boolSlice,
			expected: []bool{},
		},
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			expected: [][]bool{{true}, {false}},
			dst:      &boolSliceDim2,
		},
		{
			src: pgtype.BoolArray{
				Elements: []pgtype.Bool{
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			expected: [][][][]bool{{{{true, false, true}}}, {{{false, true, false}}}},
			dst:      &boolSliceDim4,
		},
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			expected: [2][1]bool{{true}, {false}},
			dst:      &boolArrayDim2,
		},
		{
			src: pgtype.BoolArray{
				Elements: []pgtype.Bool{
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true},
					{Bool: true, Valid: true},
					{Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			expected: [2][1][1][3]bool{{{{true, false, true}}}, {{{false, true, false}}}},
			dst:      &boolArrayDim4,
		},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); !reflect.DeepEqual(dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	errorTests := []struct {
		src pgtype.BoolArray
		dst interface{}
	}{
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &boolSlice,
		},
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &boolArrayDim2,
		},
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &boolSlice,
		},
		{
			src: pgtype.BoolArray{
				Elements:   []pgtype.Bool{{Bool: true, Valid: true}, {Bool: false, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &boolArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}

}
