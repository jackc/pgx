package pgtype_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestEnumArrayTranscode(t *testing.T) {
	setupConn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, setupConn)

	if _, err := setupConn.Exec(context.Background(), "drop type if exists color"); err != nil {
		t.Fatal(err)
	}
	if _, err := setupConn.Exec(context.Background(), "create type color as enum ('red', 'green', 'blue')"); err != nil {
		t.Fatal(err)
	}

	testutil.TestSuccessfulTranscode(t, "color[]", []interface{}{
		&pgtype.EnumArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.EnumArray{
			Elements: []pgtype.GenericText{
				{String: "red", Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.EnumArray{},
		&pgtype.EnumArray{
			Elements: []pgtype.GenericText{
				{String: "red", Valid: true},
				{String: "green", Valid: true},
				{String: "blue", Valid: true},
				{String: "red", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestEnumArrayArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.EnumArray
	}{
		{
			source: []string{"foo"},
			result: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]string)(nil)),
			result: pgtype.EnumArray{},
		},
		{
			source: [][]string{{"foo"}, {"bar"}},
			result: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.EnumArray{
				Elements: []pgtype.GenericText{
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
			result: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]string{{{{"foo", "bar", "baz"}}}, {{{"wibble", "wobble", "wubble"}}}},
			result: pgtype.EnumArray{
				Elements: []pgtype.GenericText{
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
		var r pgtype.EnumArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestEnumArrayArrayAssignTo(t *testing.T) {
	var stringSlice []string
	type _stringSlice []string
	var namedStringSlice _stringSlice
	var stringSliceDim2 [][]string
	var stringSliceDim4 [][][][]string
	var stringArrayDim2 [2][1]string
	var stringArrayDim4 [2][1][1][3]string

	simpleTests := []struct {
		src      pgtype.EnumArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &stringSlice,
			expected: []string{"foo"},
		},
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedStringSlice,
			expected: _stringSlice{"bar"},
		},
		{
			src:      pgtype.EnumArray{},
			dst:      &stringSlice,
			expected: (([]string)(nil)),
		},
		{
			src:      pgtype.EnumArray{Valid: true},
			dst:      &stringSlice,
			expected: []string{},
		},
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringSliceDim2,
			expected: [][]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.EnumArray{
				Elements: []pgtype.GenericText{
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
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringArrayDim2,
			expected: [2][1]string{{"foo"}, {"bar"}},
		},
		{
			src: pgtype.EnumArray{
				Elements: []pgtype.GenericText{
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
		src pgtype.EnumArray
		dst interface{}
	}{
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &stringSlice,
		},
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringArrayDim2,
		},
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringSlice,
		},
		{
			src: pgtype.EnumArray{
				Elements:   []pgtype.GenericText{{String: "foo", Valid: true}, {String: "bar", Valid: true}},
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
