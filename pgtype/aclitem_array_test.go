package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestACLItemArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "aclitem[]", []interface{}{
		&pgtype.ACLItemArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.ACLItemArray{
			Elements: []pgtype.ACLItem{
				{String: "=r/postgres", Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.ACLItemArray{},
		&pgtype.ACLItemArray{
			Elements: []pgtype.ACLItem{
				{String: "=r/postgres", Valid: true},
				{String: "postgres=arwdDxt/postgres", Valid: true},
				//{String: `postgres=arwdDxt/" tricky, ' } "" \ test user "`, Valid: true},
				{String: `postgres=arwdDxt/postgres`, Valid: true}, // todo: remove after fixing above case
				{String: "=r/postgres", Valid: true},
				{},
				{String: "=r/postgres", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.ACLItemArray{
			Elements: []pgtype.ACLItem{
				{String: "=r/postgres", Valid: true},
				{String: "postgres=arwdDxt/postgres", Valid: true},
				{String: "=r/postgres", Valid: true},
				{String: "postgres=arwdDxt/postgres", Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestACLItemArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.ACLItemArray
	}{
		{
			source: []string{"=r/postgres"},
			result: pgtype.ACLItemArray{
				Elements:   []pgtype.ACLItem{{String: "=r/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]string)(nil)),
			result: pgtype.ACLItemArray{},
		},
		{
			source: [][]string{{"=r/postgres"}, {"postgres=arwdDxt/postgres"}},
			result: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]string{
				{{{
					"=r/postgres",
					"postgres=arwdDxt/postgres",
					"=r/postgres"}}},
				{{{
					"postgres=arwdDxt/postgres",
					"=r/postgres",
					"postgres=arwdDxt/postgres"}}}},
			result: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1]string{{"=r/postgres"}, {"postgres=arwdDxt/postgres"}},
			result: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3]string{
				{{{
					"=r/postgres",
					"postgres=arwdDxt/postgres",
					"=r/postgres"}}},
				{{{
					"postgres=arwdDxt/postgres",
					"=r/postgres",
					"postgres=arwdDxt/postgres"}}}},
			result: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.ACLItemArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestACLItemArrayAssignTo(t *testing.T) {
	var stringSlice []string
	type _stringSlice []string
	var namedStringSlice _stringSlice
	var stringSliceDim2 [][]string
	var stringSliceDim4 [][][][]string
	var stringArrayDim2 [2][1]string
	var stringArrayDim4 [2][1][1][3]string

	simpleTests := []struct {
		src      pgtype.ACLItemArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.ACLItemArray{
				Elements:   []pgtype.ACLItem{{String: "=r/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &stringSlice,
			expected: []string{"=r/postgres"},
		},
		{
			src: pgtype.ACLItemArray{
				Elements:   []pgtype.ACLItem{{String: "=r/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &namedStringSlice,
			expected: _stringSlice{"=r/postgres"},
		},
		{
			src:      pgtype.ACLItemArray{},
			dst:      &stringSlice,
			expected: (([]string)(nil)),
		},
		{
			src:      pgtype.ACLItemArray{Valid: true},
			dst:      &stringSlice,
			expected: []string{},
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringSliceDim2,
			expected: [][]string{{"=r/postgres"}, {"postgres=arwdDxt/postgres"}},
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst: &stringSliceDim4,
			expected: [][][][]string{
				{{{
					"=r/postgres",
					"postgres=arwdDxt/postgres",
					"=r/postgres"}}},
				{{{
					"postgres=arwdDxt/postgres",
					"=r/postgres",
					"postgres=arwdDxt/postgres"}}}},
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &stringArrayDim2,
			expected: [2][1]string{{"=r/postgres"}, {"postgres=arwdDxt/postgres"}},
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true},
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst: &stringArrayDim4,
			expected: [2][1][1][3]string{
				{{{
					"=r/postgres",
					"postgres=arwdDxt/postgres",
					"=r/postgres"}}},
				{{{
					"postgres=arwdDxt/postgres",
					"=r/postgres",
					"postgres=arwdDxt/postgres"}}}},
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
		src pgtype.ACLItemArray
		dst interface{}
	}{
		{
			src: pgtype.ACLItemArray{
				Elements:   []pgtype.ACLItem{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &stringSlice,
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringArrayDim2,
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &stringSlice,
		},
		{
			src: pgtype.ACLItemArray{
				Elements: []pgtype.ACLItem{
					{String: "=r/postgres", Valid: true},
					{String: "postgres=arwdDxt/postgres", Valid: true}},
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
