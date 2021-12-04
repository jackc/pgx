package pgtype_test

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestByteaTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "bytea", []interface{}{
		&pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true},
		&pgtype.Bytea{Bytes: []byte{}, Valid: true},
		&pgtype.Bytea{Bytes: nil},
	})
}

func TestByteaSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Bytea
	}{
		{source: []byte{1, 2, 3}, result: pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true}},
		{source: []byte{}, result: pgtype.Bytea{Bytes: []byte{}, Valid: true}},
		{source: []byte(nil), result: pgtype.Bytea{}},
		{source: _byteSlice{1, 2, 3}, result: pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true}},
		{source: _byteSlice(nil), result: pgtype.Bytea{}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Bytea
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestByteaAssignTo(t *testing.T) {
	var buf []byte
	var _buf _byteSlice
	var pbuf *[]byte
	var _pbuf *_byteSlice

	simpleTests := []struct {
		src      pgtype.Bytea
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true}, dst: &buf, expected: []byte{1, 2, 3}},
		{src: pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true}, dst: &_buf, expected: _byteSlice{1, 2, 3}},
		{src: pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true}, dst: &pbuf, expected: &[]byte{1, 2, 3}},
		{src: pgtype.Bytea{Bytes: []byte{1, 2, 3}, Valid: true}, dst: &_pbuf, expected: &_byteSlice{1, 2, 3}},
		{src: pgtype.Bytea{}, dst: &pbuf, expected: ((*[]byte)(nil))},
		{src: pgtype.Bytea{}, dst: &_pbuf, expected: ((*_byteSlice)(nil))},
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
