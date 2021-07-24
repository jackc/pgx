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
			Status:     pgtype.Present,
		},
		&pgtype.ByteaArray{
			Elements: []pgtype.Bytea{
				{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.ByteaArray{Status: pgtype.Null},
		&pgtype.ByteaArray{
			Elements: []pgtype.Bytea{
				{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
				{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
				{Bytes: []byte{}, Status: pgtype.Present},
				{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
				{Status: pgtype.Null},
				{Bytes: []byte{1}, Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.ByteaArray{
			Elements: []pgtype.Bytea{
				{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
				{Bytes: []byte{}, Status: pgtype.Present},
				{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
				{Bytes: []byte{1}, Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
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
				Elements:   []pgtype.Bytea{{Bytes: []byte{1, 2, 3}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([][]byte)(nil)),
			result: pgtype.ByteaArray{Status: pgtype.Null},
		},
		{
			source: [][][]byte{{{1}}, {{2}}},
			result: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Status: pgtype.Present}, {Bytes: []byte{2}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][][][][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
			result: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
					{Bytes: []byte{4, 5, 6}, Status: pgtype.Present},
					{Bytes: []byte{7, 8, 9}, Status: pgtype.Present},
					{Bytes: []byte{10, 11, 12}, Status: pgtype.Present},
					{Bytes: []byte{13, 14, 15}, Status: pgtype.Present},
					{Bytes: []byte{16, 17, 18}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
		{
			source: [2][1][]byte{{{1}}, {{2}}},
			result: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Status: pgtype.Present}, {Bytes: []byte{2}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [2][1][1][3][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
			result: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
					{Bytes: []byte{4, 5, 6}, Status: pgtype.Present},
					{Bytes: []byte{7, 8, 9}, Status: pgtype.Present},
					{Bytes: []byte{10, 11, 12}, Status: pgtype.Present},
					{Bytes: []byte{13, 14, 15}, Status: pgtype.Present},
					{Bytes: []byte{16, 17, 18}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
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
				Elements:   []pgtype.Bytea{{Bytes: []byte{1, 2, 3}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &byteByteSlice,
			expected: [][]byte{{1, 2, 3}},
		},
		{
			src:      pgtype.ByteaArray{Status: pgtype.Null},
			dst:      &byteByteSlice,
			expected: (([][]byte)(nil)),
		},
		{
			src:      pgtype.ByteaArray{Status: pgtype.Present},
			dst:      &byteByteSlice,
			expected: [][]byte{},
		},
		{
			src: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Status: pgtype.Present}, {Bytes: []byte{2}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst:      &byteByteSliceDim2,
			expected: [][][]byte{{{1}}, {{2}}},
		},
		{
			src: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
					{Bytes: []byte{4, 5, 6}, Status: pgtype.Present},
					{Bytes: []byte{7, 8, 9}, Status: pgtype.Present},
					{Bytes: []byte{10, 11, 12}, Status: pgtype.Present},
					{Bytes: []byte{13, 14, 15}, Status: pgtype.Present},
					{Bytes: []byte{16, 17, 18}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			dst:      &byteByteSliceDim4,
			expected: [][][][][]byte{{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}}}, {{{{10, 11, 12}, {13, 14, 15}, {16, 17, 18}}}}},
		},
		{
			src: pgtype.ByteaArray{
				Elements:   []pgtype.Bytea{{Bytes: []byte{1}, Status: pgtype.Present}, {Bytes: []byte{2}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst:      &byteByteArraySliceDim2,
			expected: [2][1][]byte{{{1}}, {{2}}},
		},
		{
			src: pgtype.ByteaArray{
				Elements: []pgtype.Bytea{
					{Bytes: []byte{1, 2, 3}, Status: pgtype.Present},
					{Bytes: []byte{4, 5, 6}, Status: pgtype.Present},
					{Bytes: []byte{7, 8, 9}, Status: pgtype.Present},
					{Bytes: []byte{10, 11, 12}, Status: pgtype.Present},
					{Bytes: []byte{13, 14, 15}, Status: pgtype.Present},
					{Bytes: []byte{16, 17, 18}, Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
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
