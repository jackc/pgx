package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// https://github.com/jackc/pgx/v4/pgtype/issues/78
func TestTextArrayDecodeTextNull(t *testing.T) {
	textArray := &pgtype.TextArray{}
	err := textArray.DecodeText(nil, []byte(`{abc,"NULL",NULL,def}`))
	require.NoError(t, err)
	require.Len(t, textArray.Elements, 4)
	assert.Equal(t, true, textArray.Elements[1].Valid)
	assert.Equal(t, false, textArray.Elements[2].Valid)
}

func TestTextArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "text[]", []interface{}{
		&pgtype.TextArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				{String: "foo", Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.TextArray{},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
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
		&pgtype.TextArray{
			Elements: []pgtype.Text{
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

func TestTextArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.TextArray
	}{
		{
			source: []string{"foo"},
			result: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]string)(nil)),
			result: pgtype.TextArray{},
		},
		{
			source: [][]string{{"foo"}, {"bar"}},
			result: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.TextArray{
				Elements: []pgtype.Text{
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
			result: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.TextArray{
				Elements: []pgtype.Text{
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
		var r pgtype.TextArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestTextArrayAssignTo(t *testing.T) {
	var stringSlice []string
	type _stringSlice []string
	var namedStringSlice _stringSlice
	var stringSliceDim2 [][]string
	var stringSliceDim4 [][][][]string
	var stringArrayDim2 [2][1]string
	var stringArrayDim4 [2][1][1][3]string

	simpleTests := []struct {
		src      pgtype.TextArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &stringSlice,
			expected: []string{"foo"},
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedStringSlice,
			expected: _stringSlice{"bar"},
		},
		{
			src:      pgtype.TextArray{},
			dst:      &stringSlice,
			expected: (([]string)(nil)),
		},
		{
			src:      pgtype.TextArray{Valid: true},
			dst:      &stringSlice,
			expected: []string{},
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringSliceDim2,
			expected: [][]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.TextArray{
				Elements: []pgtype.Text{
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
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringArrayDim2,
			expected: [2][1]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.TextArray{
				Elements: []pgtype.Text{
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
		src pgtype.TextArray
		dst interface{}
	}{
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &stringSlice,
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringArrayDim2,
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringSlice,
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
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
