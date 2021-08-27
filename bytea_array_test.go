package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestByteaArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "bytea[]", []interface{}{
		&pgtype.ByteaArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.ByteaArray{
			Elements: []pgtype.Bytea{
				{Bytes: []byte{1, 2, 3}, Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.ByteaArray{},
		&pgtype.ByteaArray{
			Elements: []pgtype.Bytea{
				{Bytes: []byte{1, 2, 3}, Valid: true},
				{Bytes: []byte{1, 2, 3}, Valid: true},
				{Bytes: []byte{}, Valid: true},
				{Bytes: []byte{1, 2, 3}, Valid: true},
				{},
				{Bytes: []byte{1}, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.ByteaArray{
			Elements: []pgtype.Bytea{
				{Bytes: []byte{1, 2, 3}, Valid: true},
				{Bytes: []byte{}, Valid: true},
				{Bytes: []byte{1, 2, 3}, Valid: true},
				{Bytes: []byte{1}, Valid: true},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Valid: true,
		},
	})
}

func TestByteaArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.ByteaArray
	}{
		{
			source: [][]byte{{1, 2, 3}},
			result: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1, 2, 3}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([][]byte)(nil)),
			result: pgtype.ByteaArray{},
		},
		{
			source: [][][]byte{{{1}}, {{2}}},
			result: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Valid: true}, {Bytes: []byte{2}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [][][][][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
			result: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Valid: true},
					{Bytes: []byte{4, 5, 6}, Valid: true},
					{Bytes: []byte{7, 8, 9}, Valid: true},
					{Bytes: []byte{10, 11, 12}, Valid: true},
					{Bytes: []byte{13, 14, 15}, Valid: true},
					{Bytes: []byte{16, 17, 18}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1][]byte{{{1}}, {{2}}},
			result: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Valid: true}, {Bytes: []byte{2}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: [2][1][1][3][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
			result: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Valid: true},
					{Bytes: []byte{4, 5, 6}, Valid: true},
					{Bytes: []byte{7, 8, 9}, Valid: true},
					{Bytes: []byte{10, 11, 12}, Valid: true},
					{Bytes: []byte{13, 14, 15}, Valid: true},
					{Bytes: []byte{16, 17, 18}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.ByteaArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestByteaArrayAssignTo(t *testing.T) {
	var byteByteSlice [][]byte
	var byteByteSliceDim2 [][][]byte
	var byteByteSliceDim4 [][][][][]byte
	var byteByteArraySliceDim2 [2][1][]byte
	var byteByteArraySliceDim4 [2][1][1][3][]byte

	simpleTests := []struct {
		src      pgtype.ByteaArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1, 2, 3}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &byteByteSlice,
			expected: [][]byte{{1, 2, 3}},
		},
		{
			src:      pgtype.ByteaArray{},
			dst:      &byteByteSlice,
			expected: (([][]byte)(nil)),
		},
		{
			src:      pgtype.ByteaArray{Valid: true},
			dst:      &byteByteSlice,
			expected: [][]byte{},
		},
		{
			src: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Valid: true}, {Bytes: []byte{2}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &byteByteSliceDim2,
			expected: [][][]byte{{{1}}, {{2}}},
		},
		{
			src: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Valid: true},
					{Bytes: []byte{4, 5, 6}, Valid: true},
					{Bytes: []byte{7, 8, 9}, Valid: true},
					{Bytes: []byte{10, 11, 12}, Valid: true},
					{Bytes: []byte{13, 14, 15}, Valid: true},
					{Bytes: []byte{16, 17, 18}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst:      &byteByteSliceDim4,
			expected: [][][][][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
		},
		{
			src: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Valid: true}, {Bytes: []byte{2}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst:      &byteByteArraySliceDim2,
			expected: [2][1][]byte{{{1}}, {{2}}},
		},
		{
			src: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Valid: true},
					{Bytes: []byte{4, 5, 6}, Valid: true},
					{Bytes: []byte{7, 8, 9}, Valid: true},
					{Bytes: []byte{10, 11, 12}, Valid: true},
					{Bytes: []byte{13, 14, 15}, Valid: true},
					{Bytes: []byte{16, 17, 18}, Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
			dst:      &byteByteArraySliceDim4,
			expected: [2][1][1][3][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
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
