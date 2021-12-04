package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestFloat8ArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "float8[]", []interface{}{
		&pgtype.Float8Array{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.Float8Array{
			Elements: []pgtype.Float8{
				{Float: 1, Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.Float8Array{},
		&pgtype.Float8Array{
			Elements: []pgtype.Float8{
				{Float: 1, Valid: true},
				{Float: 2, Valid: true},
				{Float: 3, Valid: true},
				{Float: 4, Valid: true},
				{},
				{Float: 6, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.Float8Array{
			Elements: []pgtype.Float8{
				{Float: 1, Valid: true},
				{Float: 2, Valid: true},
				{Float: 3, Valid: true},
				{Float: 4, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestFloat8ArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Float8Array
	}{
		{
			source: []float64{1},
			result: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]float64)(nil)),
			result: pgtype.Float8Array{},
		},
		{
			source: [][]float64{{1}, {2}},
			result: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}, {Float: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]float64{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.Float8Array{
				Elements: []pgtype.Float8{
					{Float: 1, Valid: true},
					{Float: 2, Valid: true},
					{Float: 3, Valid: true},
					{Float: 4, Valid: true},
					{Float: 5, Valid: true},
					{Float: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.Float8Array
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestFloat8ArrayAssignTo(t *testing.T) {
	var float64Slice []float64
	var namedFloat64Slice _float64Slice
	var float64SliceDim2 [][]float64
	var float64SliceDim4 [][][][]float64
	var float64ArrayDim2 [2][1]float64
	var float64ArrayDim4 [2][1][1][3]float64

	simpleTests := []struct {
		src      pgtype.Float8Array
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1.23, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &float64Slice,
			expected: []float64{1.23},
		},
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1.23, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedFloat64Slice,
			expected: _float64Slice{1.23},
		},
		{
			src:      pgtype.Float8Array{},
			dst:      &float64Slice,
			expected: (([]float64)(nil)),
		},
		{
			src:      pgtype.Float8Array{Valid: true},
			dst:      &float64Slice,
			expected: []float64{},
		},
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}, {Float: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			expected: [][]float64{{1}, {2}},
			dst:      &float64SliceDim2,
		},
		{
			src: pgtype.Float8Array{
				Elements: []pgtype.Float8{
					{Float: 1, Valid: true},
					{Float: 2, Valid: true},
					{Float: 3, Valid: true},
					{Float: 4, Valid: true},
					{Float: 5, Valid: true},
					{Float: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			expected: [][][][]float64{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			dst:      &float64SliceDim4,
		},
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}, {Float: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			expected: [2][1]float64{{1}, {2}},
			dst:      &float64ArrayDim2,
		},
		{
			src: pgtype.Float8Array{
				Elements: []pgtype.Float8{
					{Float: 1, Valid: true},
					{Float: 2, Valid: true},
					{Float: 3, Valid: true},
					{Float: 4, Valid: true},
					{Float: 5, Valid: true},
					{Float: 6, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			expected: [2][1][1][3]float64{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			dst:      &float64ArrayDim4,
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
		src pgtype.Float8Array
		dst interface{}
	}{
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &float64Slice,
		},
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}, {Float: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &float64ArrayDim2,
		},
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}, {Float: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &float64Slice,
		},
		{
			src: pgtype.Float8Array{
				Elements:   []pgtype.Float8{{Float: 1, Valid: true}, {Float: 2, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &float64ArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}

}
