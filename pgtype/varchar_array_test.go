package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestVarcharArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "varchar[]", []interface{}{
		&pgtype.VarcharArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.VarcharArray{
			Elements: []pgtype.Varchar{
				{String: "foo", Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.VarcharArray{},
		&pgtype.VarcharArray{
			Elements: []pgtype.Varchar{
				{String: "bar ", Valid: true},
				{String: "NuLL", Valid: true},
				{String: `wow"quz\`, Valid: true},
				{String: "", Valid: true},
				{},
				{String: "null", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.VarcharArray{
			Elements: []pgtype.Varchar{
				{String: "bar", Valid: true},
				{String: "baz", Valid: true},
				{String: "quz", Valid: true},
				{String: "foo", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestVarcharArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.VarcharArray
	}{
		{
			source: []string{"foo"},
			result: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]string)(nil)),
			result: pgtype.VarcharArray{},
		},
		{
			source: [][]string{{"foo"}, {"bar"}},
			result: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.VarcharArray{
				Elements: []pgtype.Varchar{
					{String: "foo", Valid: true},
					{String: "bar", Valid: true},
					{String: "baz", Valid: true},
					{String: "wibble", Valid: true},
					{String: "wobble", Valid: true},
					{String: "wubble", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1]string{{"foo"}, {"bar"}},
			result: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.VarcharArray{
				Elements: []pgtype.Varchar{
					{String: "foo", Valid: true},
					{String: "bar", Valid: true},
					{String: "baz", Valid: true},
					{String: "wibble", Valid: true},
					{String: "wobble", Valid: true},
					{String: "wubble", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.VarcharArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestVarcharArrayAssignTo(t *testing.T) {
	var stringSlice []string
	type _stringSlice []string
	var namedStringSlice _stringSlice
	var stringSliceDim2 [][]string
	var stringSliceDim4 [][][][]string
	var stringArrayDim2 [2][1]string
	var stringArrayDim4 [2][1][1][3]string

	simpleTests := []struct {
		src      pgtype.VarcharArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &stringSlice,
			expected: []string{"foo"},
		},
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedStringSlice,
			expected: _stringSlice{"bar"},
		},
		{
			src:      pgtype.VarcharArray{},
			dst:      &stringSlice,
			expected: (([]string)(nil)),
		},
		{
			src:      pgtype.VarcharArray{Valid: true},
			dst:      &stringSlice,
			expected: []string{},
		},
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringSliceDim2,
			expected: [][]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.VarcharArray{
				Elements: []pgtype.Varchar{
					{String: "foo", Valid: true},
					{String: "bar", Valid: true},
					{String: "baz", Valid: true},
					{String: "wibble", Valid: true},
					{String: "wobble", Valid: true},
					{String: "wubble", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst:      &stringSliceDim4,
			expected: [][][][]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
		},
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringArrayDim2,
			expected: [2][1]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.VarcharArray{
				Elements: []pgtype.Varchar{
					{String: "foo", Valid: true},
					{String: "bar", Valid: true},
					{String: "baz", Valid: true},
					{String: "wibble", Valid: true},
					{String: "wobble", Valid: true},
					{String: "wubble", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst:      &stringArrayDim4,
			expected: [2][1][1][3]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
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
		src pgtype.VarcharArray
		dst interface{}
	}{
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &stringSlice,
		},
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringArrayDim2,
		},
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringSlice,
		},
		{
			src: pgtype.VarcharArray{
				Elements:   []pgtype.Varchar{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &stringArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
