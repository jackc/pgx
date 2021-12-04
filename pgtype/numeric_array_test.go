package pgtype_test

import (
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestNumericArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "numeric[]", []interface{}{
		&pgtype.NumericArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.NumericArray{
			Elements: []pgtype.Numeric{
				{Int: big.NewInt(1), Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.NumericArray{},
		&pgtype.NumericArray{
			Elements: []pgtype.Numeric{
				{Int: big.NewInt(1), Valid: true},
				{Int: big.NewInt(2), Valid: true},
				{Int: big.NewInt(3), Valid: true},
				{Int: big.NewInt(4), Valid: true},
				{},
				{Int: big.NewInt(6), Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.NumericArray{
			Elements: []pgtype.Numeric{
				{Int: big.NewInt(1), Valid: true},
				{Int: big.NewInt(2), Valid: true},
				{Int: big.NewInt(3), Valid: true},
				{Int: big.NewInt(4), Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestNumericArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.NumericArray
	}{
		{
			source: []float32{1},
			result: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []float32{float32(math.Copysign(0, -1))},
			result: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(0), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []float64{1},
			result: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: []float64{math.Copysign(0, -1)},
			result: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(0), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]float32)(nil)),
			result: pgtype.NumericArray{},
		},
		{
			source: [][]float32{{1}, {2}},
			result: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]float32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.NumericArray{
				Elements: []pgtype.Numeric{
					{Int: big.NewInt(1), Valid: true},
					{Int: big.NewInt(2), Valid: true},
					{Int: big.NewInt(3), Valid: true},
					{Int: big.NewInt(4), Valid: true},
					{Int: big.NewInt(5), Valid: true},
					{Int: big.NewInt(6), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1]float32{{1}, {2}},
			result: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]float32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
			result: pgtype.NumericArray{
				Elements: []pgtype.Numeric{
					{Int: big.NewInt(1), Valid: true},
					{Int: big.NewInt(2), Valid: true},
					{Int: big.NewInt(3), Valid: true},
					{Int: big.NewInt(4), Valid: true},
					{Int: big.NewInt(5), Valid: true},
					{Int: big.NewInt(6), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.NumericArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestNumericArrayAssignTo(t *testing.T) {
	var float32Slice []float32
	var float64Slice []float64
	var float32SliceDim2 [][]float32
	var float32SliceDim4 [][][][]float32
	var float32ArrayDim2 [2][1]float32
	var float32ArrayDim4 [2][1][1][3]float32

	simpleTests := []struct {
		src      pgtype.NumericArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &float32Slice,
			expected: []float32{1},
		},
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &float64Slice,
			expected: []float64{1},
		},
		{
			src:      pgtype.NumericArray{},
			dst:      &float32Slice,
			expected: (([]float32)(nil)),
		},
		{
			src:      pgtype.NumericArray{Valid: true},
			dst:      &float32Slice,
			expected: []float32{},
		},
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &float32SliceDim2,
			expected: [][]float32{{1}, {2}},
		},
		{
			src: pgtype.NumericArray{
				Elements: []pgtype.Numeric{
					{Int: big.NewInt(1), Valid: true},
					{Int: big.NewInt(2), Valid: true},
					{Int: big.NewInt(3), Valid: true},
					{Int: big.NewInt(4), Valid: true},
					{Int: big.NewInt(5), Valid: true},
					{Int: big.NewInt(6), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst:      &float32SliceDim4,
			expected: [][][][]float32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
		},
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &float32ArrayDim2,
			expected: [2][1]float32{{1}, {2}},
		},
		{
			src: pgtype.NumericArray{
				Elements: []pgtype.Numeric{
					{Int: big.NewInt(1), Valid: true},
					{Int: big.NewInt(2), Valid: true},
					{Int: big.NewInt(3), Valid: true},
					{Int: big.NewInt(4), Valid: true},
					{Int: big.NewInt(5), Valid: true},
					{Int: big.NewInt(6), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst:      &float32ArrayDim4,
			expected: [2][1][1][3]float32{{{{1, 2, 3}}}, {{{4, 5, 6}}}},
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
		src pgtype.NumericArray
		dst interface{}
	}{
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &float32Slice,
		},
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &float32ArrayDim2,
		},
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &float32Slice,
		},
		{
			src: pgtype.NumericArray{
				Elements:   []pgtype.Numeric{{Int: big.NewInt(1), Valid: true}, {Int: big.NewInt(2), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &float32ArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}

}
