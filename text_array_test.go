package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// https://github.com/jackc/pgtype/issues/78
func TestTextArrayDecodeTextNull(t *testing.T) {
	textArray := &pgtype.TextArray{}
	err := textArray.DecodeText(nil, []byte(`{abc,"NULL",NULL,def}`))
	require.NoError(t, err)
	require.Len(t, textArray.Elements, 4)
	assert.Equal(t, pgtype.Present, textArray.Elements[1].Status)
	assert.Equal(t, pgtype.Null, textArray.Elements[2].Status)
}

func TestTextArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "text[]", []interface{}{
		&pgtype.TextArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				{String: "foo", Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.TextArray{Status: pgtype.Null},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				{String: "bar ", Status: pgtype.Present},
				{String: "NuLL", Status: pgtype.Present},
				{String: `wow"quz\`, Status: pgtype.Present},
				{String: "", Status: pgtype.Present},
				{Status: pgtype.Null},
				{String: "null", Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				{String: "bar", Status: pgtype.Present},
				{String: "baz", Status: pgtype.Present},
				{String: "quz", Status: pgtype.Present},
				{String: "foo", Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
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
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([]string)(nil)),
			result: pgtype.TextArray{Status: pgtype.Null},
		},
		{
			source: [][]string{{"foo"}, {"bar"}},
			result: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][][][]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.TextArray{
				Elements: []pgtype.Text{
					{String: "foo", Status: pgtype.Present},
					{String: "bar", Status: pgtype.Present},
					{String: "baz", Status: pgtype.Present},
					{String: "wibble", Status: pgtype.Present},
					{String: "wobble", Status: pgtype.Present},
					{String: "wubble", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
		{
			source: [2][1]string{{"foo"}, {"bar"}},
			result: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [2][1][1][3]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.TextArray{
				Elements: []pgtype.Text{
					{String: "foo", Status: pgtype.Present},
					{String: "bar", Status: pgtype.Present},
					{String: "baz", Status: pgtype.Present},
					{String: "wibble", Status: pgtype.Present},
					{String: "wobble", Status: pgtype.Present},
					{String: "wubble", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
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
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &stringSlice,
			expected: []string{"foo"},
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &namedStringSlice,
			expected: _stringSlice{"bar"},
		},
		{
			src:      pgtype.TextArray{Status: pgtype.Null},
			dst:      &stringSlice,
			expected: (([]string)(nil)),
		},
		{
			src:      pgtype.TextArray{Status: pgtype.Present},
			dst:      &stringSlice,
			expected: []string{},
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst:      &stringSliceDim2,
			expected: [][]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.TextArray{
				Elements: []pgtype.Text{
					{String: "foo", Status: pgtype.Present},
					{String: "bar", Status: pgtype.Present},
					{String: "baz", Status: pgtype.Present},
					{String: "wibble", Status: pgtype.Present},
					{String: "wobble", Status: pgtype.Present},
					{String: "wubble", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			dst:      &stringSliceDim4,
			expected: [][][][]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst:      &stringArrayDim2,
			expected: [2][1]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.TextArray{
				Elements: []pgtype.Text{
					{String: "foo", Status: pgtype.Present},
					{String: "bar", Status: pgtype.Present},
					{String: "baz", Status: pgtype.Present},
					{String: "wibble", Status: pgtype.Present},
					{String: "wobble", Status: pgtype.Present},
					{String: "wubble", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
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
				Elements:   []pgtype.Text{{Status: pgtype.Null}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst: &stringSlice,
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Status:     pgtype.Present},
			dst: &stringArrayDim2,
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Status:     pgtype.Present},
			dst: &stringSlice,
		},
		{
			src: pgtype.TextArray{
				Elements:   []pgtype.Text{{String: "foo", Status: pgtype.Present}, {String: "bar", Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
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
