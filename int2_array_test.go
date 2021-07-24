package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestInt2ArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int2[]", []interface{}{
		&pgtype.Int2Array{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.Int2Array{
			Elements: []pgtype.Int2{
				{Int: 1, Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.Int2Array{Status: pgtype.Null},
		&pgtype.Int2Array{
			Elements: []pgtype.Int2{
				{Int: 1, Status: pgtype.Present},
				{Int: 2, Status: pgtype.Present},
				{Int: 3, Status: pgtype.Present},
				{Int: 4, Status: pgtype.Present},
				{Status: pgtype.Null},
				{Int: 6, Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.Int2Array{
			Elements: []pgtype.Int2{
				{Int: 1, Status: pgtype.Present},
				{Int: 2, Status: pgtype.Present},
				{Int: 3, Status: pgtype.Present},
				{Int: 4, Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
		},
	})
}

func TestInt2ArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Int2Array
	}{
		{
			source: []int64{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []int32{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []int16{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []int{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []uint64{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []uint32{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []uint16{1},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([]int16)(nil)),
			result: pgtype.Int2Array{Status: pgtype.Null},
		},
		{
			source: [][]int16{{1}, {2}},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][][][]int16{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.Int2Array{
				Elements: []pgtype.Int2{
					{Int: 1, Status: pgtype.Present},
					{Int: 2, Status: pgtype.Present},
					{Int: 3, Status: pgtype.Present},
					{Int: 4, Status: pgtype.Present},
					{Int: 5, Status: pgtype.Present},
					{Int: 6, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
		{
			source: [2][1]int16{{1}, {2}},
			result: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [2][1][1][3]int16{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.Int2Array{
				Elements: []pgtype.Int2{
					{Int: 1, Status: pgtype.Present},
					{Int: 2, Status: pgtype.Present},
					{Int: 3, Status: pgtype.Present},
					{Int: 4, Status: pgtype.Present},
					{Int: 5, Status: pgtype.Present},
					{Int: 6, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.Int2Array
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestInt2ArrayAssignTo(t *testing.T) {
	var int16Slice []int16
	var uint16Slice []uint16
	var namedInt16Slice _int16Slice
	var int16SliceDim2 [][]int16
	var int16SliceDim4 [][][][]int16
	var int16ArrayDim2 [2][1]int16
	var int16ArrayDim4 [2][1][1][3]int16

	simpleTests := []struct {
		src      pgtype.Int2Array
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &int16Slice,
			expected: []int16{1},
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &uint16Slice,
			expected: []uint16{1},
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &namedInt16Slice,
			expected: _int16Slice{1},
		},
		{
			src:      pgtype.Int2Array{Status: pgtype.Null},
			dst:      &int16Slice,
			expected: (([]int16)(nil)),
		},
		{
			src:      pgtype.Int2Array{Status: pgtype.Present},
			dst:      &int16Slice,
			expected: []int16{},
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			expected: [][]int16{{1}, {2}},
			dst:      &int16SliceDim2,
		},
		{
			src: pgtype.Int2Array{
				Elements: []pgtype.Int2{
					{Int: 1, Status: pgtype.Present},
					{Int: 2, Status: pgtype.Present},
					{Int: 3, Status: pgtype.Present},
					{Int: 4, Status: pgtype.Present},
					{Int: 5, Status: pgtype.Present},
					{Int: 6, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			expected: [][][][]int16{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			dst:      &int16SliceDim4,
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			expected: [2][1]int16{{1}, {2}},
			dst:      &int16ArrayDim2,
		},
		{
			src: pgtype.Int2Array{
				Elements: []pgtype.Int2{
					{Int: 1, Status: pgtype.Present},
					{Int: 2, Status: pgtype.Present},
					{Int: 3, Status: pgtype.Present},
					{Int: 4, Status: pgtype.Present},
					{Int: 5, Status: pgtype.Present},
					{Int: 6, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			expected: [2][1][1][3]int16{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			dst:      &int16ArrayDim4,
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
		src pgtype.Int2Array
		dst interface{}
	}{
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Status: pgtype.Null}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst: &int16Slice,
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: -1, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst: &uint16Slice,
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Status:     pgtype.Present},
			dst: &int16ArrayDim2,
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Status:     pgtype.Present},
			dst: &int16Slice,
		},
		{
			src: pgtype.Int2Array{
				Elements:   []pgtype.Int2{{Int: 1, Status: pgtype.Present}, {Int: 2, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &int16ArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}

}
