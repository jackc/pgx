package pgtype_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestTimestamptzArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "timestamptz[]", []interface{}{
		&pgtype.TimestamptzArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.TimestamptzArray{
			Elements: []pgtype.Timestamptz{
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.TimestamptzArray{Status: pgtype.Null},
		&pgtype.TimestamptzArray{
			Elements: []pgtype.Timestamptz{
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Time: time.Date(2016, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Time: time.Date(2017, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Time: time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Status: pgtype.Null},
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.TimestamptzArray{
			Elements: []pgtype.Timestamptz{
				{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Time: time.Date(2015, 2, 2, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Time: time.Date(2015, 2, 3, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				{Time: time.Date(2015, 2, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
		},
	}, func(a, b interface{}) bool {
		ata := a.(pgtype.TimestamptzArray)
		bta := b.(pgtype.TimestamptzArray)

		if len(ata.Elements) != len(bta.Elements) || ata.Status != bta.Status {
			return false
		}

		for i := range ata.Elements {
			ae, be := ata.Elements[i], bta.Elements[i]
			if !(ae.Time.Equal(be.Time) && ae.Status == be.Status && ae.InfinityModifier == be.InfinityModifier) {
				return false
			}
		}

		return true
	})
}

func TestTimestamptzArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.TimestamptzArray
	}{
		{
			source: []time.Time{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
			result: pgtype.TimestamptzArray{
				Elements:   []pgtype.Timestamptz{{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([]time.Time)(nil)),
			result: pgtype.TimestamptzArray{Status: pgtype.Null},
		},
		{
			source: [][]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
			result: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
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
			result: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
		{
			source: [2][1]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
			result: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [2][1][1][3]time.Time{
				{{{
					time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC),
					time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC)}}},
				{{{
					time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC),
					time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC),
					time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC)}}}},
			result: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.TimestamptzArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestTimestamptzArrayAssignTo(t *testing.T) {
	var timeSlice []time.Time
	var timeSliceDim2 [][]time.Time
	var timeSliceDim4 [][][][]time.Time
	var timeArrayDim2 [2][1]time.Time
	var timeArrayDim4 [2][1][1][3]time.Time

	simpleTests := []struct {
		src      pgtype.TimestamptzArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.TimestamptzArray{
				Elements:   []pgtype.Timestamptz{{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &timeSlice,
			expected: []time.Time{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
		},
		{
			src:      pgtype.TimestamptzArray{Status: pgtype.Null},
			dst:      &timeSlice,
			expected: (([]time.Time)(nil)),
		},
		{
			src:      pgtype.TimestamptzArray{Status: pgtype.Present},
			dst:      &timeSlice,
			expected: []time.Time{},
		},
		{
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &timeSliceDim2,
			expected: [][]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
		},
		{
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
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
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &timeArrayDim2,
			expected: [2][1]time.Time{
				{time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC)},
				{time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC)}},
		},
		{
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2017, 5, 6, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2018, 7, 8, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2019, 9, 10, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
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
		src pgtype.TimestamptzArray
		dst interface{}
	}{
		{
			src: pgtype.TimestamptzArray{
				Elements:   []pgtype.Timestamptz{{Status: pgtype.Null}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst: &timeSlice,
		},
		{
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Status:     pgtype.Present},
			dst: &timeArrayDim2,
		},
		{
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}, {LowerBound: 1, Length: 2}},
				Status:     pgtype.Present},
			dst: &timeSlice,
		},
		{
			src: pgtype.TimestamptzArray{
				Elements: []pgtype.Timestamptz{
					{Time: time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
					{Time: time.Date(2016, 3, 4, 0, 0, 0, 0, time.UTC), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
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
