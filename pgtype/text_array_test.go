package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/pgtype/testutil"
)

func TestTextArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "text[]", []interface{}{
		&pgtype.TextArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				pgtype.Text{String: "foo", Status: pgtype.Present},
				pgtype.Text{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.TextArray{Status: pgtype.Null},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				pgtype.Text{String: "bar ", Status: pgtype.Present},
				pgtype.Text{String: "NuLL", Status: pgtype.Present},
				pgtype.Text{String: `wow"quz\`, Status: pgtype.Present},
				pgtype.Text{String: "", Status: pgtype.Present},
				pgtype.Text{Status: pgtype.Null},
				pgtype.Text{String: "null", Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.TextArray{
			Elements: []pgtype.Text{
				pgtype.Text{String: "bar", Status: pgtype.Present},
				pgtype.Text{String: "baz", Status: pgtype.Present},
				pgtype.Text{String: "quz", Status: pgtype.Present},
				pgtype.Text{String: "foo", Status: pgtype.Present},
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
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
