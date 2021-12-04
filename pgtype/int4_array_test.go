package pgtype_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestInt4ArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int4[]", []interface{}{
		&pgtype.Int4Array{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.Int4Array{
			Elements: []pgtype.Int4{
				{Int: 1, Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.Int4Array{},
		&pgtype.Int4Array{
			Elements: []pgtype.Int4{
				{Int: 1, Valid: true},
				{Int: 2, Valid: true},
				{Int: 3, Valid: true},
				{Int: 4, Valid: true},
				{},
				{Int: 6, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.Int4Array{
			Elements: []pgtype.Int4{
				{Int: 1, Valid: true},
				{Int: 2, Valid: true},
				{Int: 3, Valid: true},
				{Int: 4, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestInt4ArraySet(t *testing.T) {
	successfulTests := []struct {
		source        interface{}
		result        pgtype.Int4Array
		expectedError bool
	}{
		{
			source: []int64{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []int32{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []int16{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []int{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source:        []int{1, math.MaxInt32 + 1, 2},
			expectedError: true,
		},
		{
			source: []uint64{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []uint32{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []uint16{1},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]int32)(nil)),
			result: pgtype.Int4Array{},
		},
		{
			source: [][]int32{{1}, {2}},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]int32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.Int4Array{
				Elements: []pgtype.Int4{
					{Int: 1, Valid: true},
					{Int: 2, Valid: true},
					{Int: 3, Valid: true},
					{Int: 4, Valid: true},
					{Int: 5, Valid: true},
					{Int: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1]int32{{1}, {2}},
			result: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]int32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.Int4Array{
				Elements: []pgtype.Int4{
					{Int: 1, Valid: true},
					{Int: 2, Valid: true},
					{Int: 3, Valid: true},
					{Int: 4, Valid: true},
					{Int: 5, Valid: true},
					{Int: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.Int4Array
		err := r.Set(tt.source)
		if err != nil {
			if tt.expectedError {
				continue
			}
			t.Errorf("%d: %v", i, err)
		}

		if tt.expectedError {
			t.Errorf("%d: an error was expected, %v", i, tt)
			continue
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestInt4ArrayAssignTo(t *testing.T) {
	var int32Slice []int32
	var uint32Slice []uint32
	var namedInt32Slice _int32Slice
	var int32SliceDim2 [][]int32
	var int32SliceDim4 [][][][]int32
	var int32ArrayDim2 [2][1]int32
	var int32ArrayDim4 [2][1][1][3]int32

	simpleTests := []struct {
		src      pgtype.Int4Array
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &int32Slice,
			expected: []int32{1},
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &uint32Slice,
			expected: []uint32{1},
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedInt32Slice,
			expected: _int32Slice{1},
		},
		{
			src:      pgtype.Int4Array{},
			dst:      &int32Slice,
			expected: (([]int32)(nil)),
		},
		{
			src:      pgtype.Int4Array{Valid: true},
			dst:      &int32Slice,
			expected: []int32{},
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			expected: [][]int32{{1}, {2}},
			dst:      &int32SliceDim2,
		},
		{
			src: pgtype.Int4Array{
				Elements: []pgtype.Int4{
					{Int: 1, Valid: true},
					{Int: 2, Valid: true},
					{Int: 3, Valid: true},
					{Int: 4, Valid: true},
					{Int: 5, Valid: true},
					{Int: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			expected: [][][][]int32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			dst:      &int32SliceDim4,
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			expected: [2][1]int32{{1}, {2}},
			dst:      &int32ArrayDim2,
		},
		{
			src: pgtype.Int4Array{
				Elements: []pgtype.Int4{
					{Int: 1, Valid: true},
					{Int: 2, Valid: true},
					{Int: 3, Valid: true},
					{Int: 4, Valid: true},
					{Int: 5, Valid: true},
					{Int: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			expected: [2][1][1][3]int32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			dst:      &int32ArrayDim4,
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
		src pgtype.Int4Array
		dst interface{}
	}{
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &int32Slice,
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: -1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &uint32Slice,
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &int32ArrayDim2,
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &int32Slice,
		},
		{
			src: pgtype.Int4Array{
				Elements:   []pgtype.Int4{{Int: 1, Valid: true}, {Int: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &int32ArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}

}
