package pgtype_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestTimestampArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "timestamp[]", []interface{}{
		&pgtype.TimestampArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.TimestampArray{
			Elements: []pgtype.Timestamp{
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.TimestampArray{},
		&pgtype.TimestampArray{
			Elements: []pgtype.Timestamp{
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				{Time: time.Date(2016, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				{Time: time.Date(2017, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				{Time: time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				{},
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.TimestampArray{
			Elements: []pgtype.Timestamp{
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				{Time: time.Date(2015, 2, 2, 0, 0, 0, 0, time.UTC), Valid: true},
				{Time: time.Date(2015, 2, 3, 0, 0, 0, 0, time.UTC), Valid: true},
				{Time: time.Date(2015, 2, 4, 0, 0, 0, 0, time.UTC), Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	}, func(a, b interface{}) bool {
		ata := a.(pgtype.TimestampArray)
		bta := b.(pgtype.TimestampArray)

		if len(ata.Elements) != len(bta.Elements) || ata.Valid != bta.Valid {
			return false
		}

		for i := range ata.Elements {
			ae, be := ata.Elements[i], bta.Elements[i]
			if !(ae.Time.Equal(be.Time) && ae.Valid == be.Valid && ae.InfinityModifier == be.InfinityModifier) {
				return false
			}
		}

		return true
	})
}

func TestTimestampArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.TimestampArray
	}{
		{
			source: []time.Time{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
			result: pgtype.TimestampArray{
				Elements:   []pgtype.Timestamp{{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]time.Time)(nil)),
			result: pgtype.TimestampArray{},
		},
		{
			source: [][]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
			result: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][]time.Time{
				{{{
					time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC),
					time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC)}}},
				{{{
					time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC),
					time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC),
					time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC)}}}},
			result: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.TimestampArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestTimestampArrayAssignTo(t *testing.T) {
	var timeSlice []time.Time
	var timeSliceDim2 [][]time.Time
	var timeSliceDim4 [][][][]time.Time
	var timeArrayDim2 [2][1]time.Time
	var timeArrayDim4 [2][1][1][3]time.Time

	simpleTests := []struct {
		src      pgtype.TimestampArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.TimestampArray{
				Elements:   []pgtype.Timestamp{{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &timeSlice,
			expected: []time.Time{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
		},
		{
			src:      pgtype.TimestampArray{},
			dst:      &timeSlice,
			expected: (([]time.Time)(nil)),
		},
		{
			src:      pgtype.TimestampArray{Valid: true},
			dst:      &timeSlice,
			expected: []time.Time{},
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &timeSliceDim2,
			expected: [][]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst: &timeSliceDim4,
			expected: [][][][]time.Time{
				{{{
					time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC),
					time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC)}}},
				{{{
					time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC),
					time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC),
					time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC)}}}},
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &timeArrayDim2,
			expected: [2][1]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst: &timeArrayDim4,
			expected: [2][1][1][3]time.Time{
				{{{
					time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC),
					time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC)}}},
				{{{
					time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC),
					time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC),
					time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC)}}}},
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
		src pgtype.TimestampArray
		dst interface{}
	}{
		{
			src: pgtype.TimestampArray{
				Elements:   []pgtype.Timestamp{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst: &timeSlice,
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &timeArrayDim2,
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Valid:      true},
			dst: &timeSlice,
		},
		{
			src: pgtype.TimestampArray{
				Elements: []pgtype.Timestamp{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Valid: true},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &timeArrayDim4,
		},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}

}
