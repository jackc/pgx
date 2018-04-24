package pgtype_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/pgtype/testutil"
)

func TestUint8Transcode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "numeric", []interface{}{
		&pgtype.Uint8{Uint: math.MaxUint64, Status: pgtype.Present},
		&pgtype.Uint8{Uint: 0, Status: pgtype.Present},
		&pgtype.Uint8{Uint: 1, Status: pgtype.Present},
		&pgtype.Uint8{Uint: math.MaxInt64, Status: pgtype.Present},
		&pgtype.Uint8{Uint: 0, Status: pgtype.Null},
	})
}

func TestUint8Set(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Uint8
	}{
		{source: int8(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: int16(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: int32(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: int64(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: uint8(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: uint16(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: uint32(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: uint64(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: "1", result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
		{source: _uint8(1), result: pgtype.Uint8{Uint: 1, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Uint8
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestUint8AssignTo(t *testing.T) {
	var i8 int8
	var i16 int16
	var i32 int32
	var i64 int64
	var i int
	var ui8 uint8
	var ui16 uint16
	var ui32 uint32
	var ui64 uint64
	var ui uint
	var pi8 *int8
	var _ui8 _uint8
	var _pui8 *_uint8

	simpleTests := []struct {
		src      pgtype.Uint8
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &i8, expected: int8(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &i16, expected: int16(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &i32, expected: int32(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &i64, expected: int64(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &i, expected: int(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &ui8, expected: uint8(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &ui16, expected: uint16(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &ui32, expected: uint32(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &ui64, expected: uint64(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &ui, expected: uint(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &_ui8, expected: _uint8(42)},
		{src: pgtype.Uint8{Uint: 0, Status: pgtype.Null}, dst: &pi8, expected: ((*int8)(nil))},
		{src: pgtype.Uint8{Uint: 0, Status: pgtype.Null}, dst: &_pui8, expected: ((*_uint8)(nil))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	pointerAllocTests := []struct {
		src      pgtype.Uint8
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &pi8, expected: int8(42)},
		{src: pgtype.Uint8{Uint: 42, Status: pgtype.Present}, dst: &_pui8, expected: _uint8(42)},
	}

	for i, tt := range pointerAllocTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	errorTests := []struct {
		src pgtype.Uint8
		dst interface{}
	}{
		{src: pgtype.Uint8{Uint: 150, Status: pgtype.Present}, dst: &i8},
		{src: pgtype.Uint8{Uint: 40000, Status: pgtype.Present}, dst: &i16},
		{src: pgtype.Uint8{Uint: 5000000000, Status: pgtype.Present}, dst: &i32},
		{src: pgtype.Uint8{Uint: 0, Status: pgtype.Null}, dst: &i64},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
