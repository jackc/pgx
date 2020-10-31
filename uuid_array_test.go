package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestUUIDArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "uuid[]", []interface{}{
		&pgtype.UUIDArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.UUIDArray{
			Elements: []pgtype.UUID{
				{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.UUIDArray{Status: pgtype.Null},
		&pgtype.UUIDArray{
			Elements: []pgtype.UUID{
				{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
				{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
				{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
				{Bytes: [16]byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63}, Status: pgtype.Present},
				{Status: pgtype.Null},
				{Bytes: [16]byte{64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79}, Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.UUIDArray{
			Elements: []pgtype.UUID{
				{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
				{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
				{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
				{Bytes: [16]byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63}, Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
		},
	})
}

func TestUUIDArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.UUIDArray
	}{
		{
			source: nil,
			result: pgtype.UUIDArray{Status: pgtype.Null},
		},
		{
			source: [][16]byte{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
			result: pgtype.UUIDArray{
				Elements:   []pgtype.UUID{{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][16]byte{},
			result: pgtype.UUIDArray{Status: pgtype.Present},
		},
		{
			source: ([][16]byte)(nil),
			result: pgtype.UUIDArray{Status: pgtype.Null},
		},
		{
			source: [][]byte{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
			result: pgtype.UUIDArray{
				Elements:   []pgtype.UUID{{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][]byte{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
				{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
				nil,
				{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47},
			},
			result: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
					{Status: pgtype.Null},
					{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 4}},
				Status:     pgtype.Present},
		},
		{
			source: [][]byte{},
			result: pgtype.UUIDArray{Status: pgtype.Present},
		},
		{
			source: ([][]byte)(nil),
			result: pgtype.UUIDArray{Status: pgtype.Null},
		},
		{
			source: []string{"00010203-0405-0607-0809-0a0b0c0d0e0f"},
			result: pgtype.UUIDArray{
				Elements:   []pgtype.UUID{{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: []string{},
			result: pgtype.UUIDArray{Status: pgtype.Present},
		},
		{
			source: ([]string)(nil),
			result: pgtype.UUIDArray{Status: pgtype.Null},
		},
		{
			source: [][][16]byte{{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
				{{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}}},
			result: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][][][]string{
				{{{
					"00010203-0405-0607-0809-0a0b0c0d0e0f",
					"10111213-1415-1617-1819-1a1b1c1d1e1f",
					"20212223-2425-2627-2829-2a2b2c2d2e2f"}}},
				{{{
					"30313233-3435-3637-3839-3a3b3c3d3e3f",
					"40414243-4445-4647-4849-4a4b4c4d4e4f",
					"50515253-5455-5657-5859-5a5b5c5d5e5f"}}}},
			result: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
					{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
					{Bytes: [16]byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63}, Status: pgtype.Present},
					{Bytes: [16]byte{64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79}, Status: pgtype.Present},
					{Bytes: [16]byte{80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
		{
			source: [2][1][16]byte{{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
				{{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}}},
			result: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [2][1][1][3]string{
				{{{
					"00010203-0405-0607-0809-0a0b0c0d0e0f",
					"10111213-1415-1617-1819-1a1b1c1d1e1f",
					"20212223-2425-2627-2829-2a2b2c2d2e2f"}}},
				{{{
					"30313233-3435-3637-3839-3a3b3c3d3e3f",
					"40414243-4445-4647-4849-4a4b4c4d4e4f",
					"50515253-5455-5657-5859-5a5b5c5d5e5f"}}}},
			result: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
					{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
					{Bytes: [16]byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63}, Status: pgtype.Present},
					{Bytes: [16]byte{64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79}, Status: pgtype.Present},
					{Bytes: [16]byte{80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.UUIDArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestUUIDArrayAssignTo(t *testing.T) {
	var byteArraySlice [][16]byte
	var byteSliceSlice [][]byte
	var stringSlice []string
	var byteSlice []byte
	var byteArraySliceDim2 [][][16]byte
	var stringSliceDim4 [][][][]string
	var byteArrayDim2 [2][1][16]byte
	var stringArrayDim4 [2][1][1][3]string

	simpleTests := []struct {
		src      pgtype.UUIDArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.UUIDArray{
				Elements:   []pgtype.UUID{{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &byteArraySlice,
			expected: [][16]byte{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		},
		{
			src:      pgtype.UUIDArray{Status: pgtype.Null},
			dst:      &byteArraySlice,
			expected: ([][16]byte)(nil),
		},
		{
			src: pgtype.UUIDArray{
				Elements:   []pgtype.UUID{{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &byteSliceSlice,
			expected: [][]byte{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		},
		{
			src:      pgtype.UUIDArray{Status: pgtype.Null},
			dst:      &byteSliceSlice,
			expected: ([][]byte)(nil),
		},
		{
			src:      pgtype.UUIDArray{Status: pgtype.Present},
			dst:      &byteSlice,
			expected: []byte{},
		},
		{
			src:      pgtype.UUIDArray{Status: pgtype.Present},
			dst:      &stringSlice,
			expected: []string{},
		},
		{
			src: pgtype.UUIDArray{
				Elements:   []pgtype.UUID{{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &stringSlice,
			expected: []string{"00010203-0405-0607-0809-0a0b0c0d0e0f"},
		},
		{
			src:      pgtype.UUIDArray{Status: pgtype.Null},
			dst:      &stringSlice,
			expected: ([]string)(nil),
		},
		{
			src: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &byteArraySliceDim2,
			expected: [][][16]byte{{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
				{{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}}},
		},
		{
			src: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
					{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
					{Bytes: [16]byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63}, Status: pgtype.Present},
					{Bytes: [16]byte{64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79}, Status: pgtype.Present},
					{Bytes: [16]byte{80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			dst: &stringSliceDim4,
			expected: [][][][]string{
				{{{
					"00010203-0405-0607-0809-0a0b0c0d0e0f",
					"10111213-1415-1617-1819-1a1b1c1d1e1f",
					"20212223-2425-2627-2829-2a2b2c2d2e2f"}}},
				{{{
					"30313233-3435-3637-3839-3a3b3c3d3e3f",
					"40414243-4445-4647-4849-4a4b4c4d4e4f",
					"50515253-5455-5657-5859-5a5b5c5d5e5f"}}}},
		},
		{
			src: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &byteArrayDim2,
			expected: [2][1][16]byte{{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
				{{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}}},
		},
		{
			src: pgtype.UUIDArray{
				Elements: []pgtype.UUID{
					{Bytes: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, Status: pgtype.Present},
					{Bytes: [16]byte{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}, Status: pgtype.Present},
					{Bytes: [16]byte{32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}, Status: pgtype.Present},
					{Bytes: [16]byte{48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63}, Status: pgtype.Present},
					{Bytes: [16]byte{64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79}, Status: pgtype.Present},
					{Bytes: [16]byte{80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			dst: &stringArrayDim4,
			expected: [2][1][1][3]string{
				{{{
					"00010203-0405-0607-0809-0a0b0c0d0e0f",
					"10111213-1415-1617-1819-1a1b1c1d1e1f",
					"20212223-2425-2627-2829-2a2b2c2d2e2f"}}},
				{{{
					"30313233-3435-3637-3839-3a3b3c3d3e3f",
					"40414243-4445-4647-4849-4a4b4c4d4e4f",
					"50515253-5455-5657-5859-5a5b5c5d5e5f"}}}},
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
}
