// Code generated from pgtype/int_test.go.erb. DO NOT EDIT.

package pgtype_test

import (
	"context"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestInt2Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int2", []pgxtest.ValueRoundTripTest{
		{Param: int8(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: int16(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: int32(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: int64(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: uint8(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: uint16(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: uint32(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: uint64(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: int(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: uint(1), Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: pgtype.Int2{Int16: 1, Valid: true}, Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: int32(-1), Result: new(pgtype.Int2), Test: isExpectedEq(pgtype.Int2{Int16: -1, Valid: true})},
		{Param: 1, Result: new(int8), Test: isExpectedEq(int8(1))},
		{Param: 1, Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: 1, Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: 1, Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: 1, Result: new(uint8), Test: isExpectedEq(uint8(1))},
		{Param: 1, Result: new(uint16), Test: isExpectedEq(uint16(1))},
		{Param: 1, Result: new(uint32), Test: isExpectedEq(uint32(1))},
		{Param: 1, Result: new(uint64), Test: isExpectedEq(uint64(1))},
		{Param: 1, Result: new(int), Test: isExpectedEq(int(1))},
		{Param: 1, Result: new(uint), Test: isExpectedEq(uint(1))},
		{Param: -1, Result: new(int8), Test: isExpectedEq(int8(-1))},
		{Param: -1, Result: new(int16), Test: isExpectedEq(int16(-1))},
		{Param: -1, Result: new(int32), Test: isExpectedEq(int32(-1))},
		{Param: -1, Result: new(int64), Test: isExpectedEq(int64(-1))},
		{Param: -1, Result: new(int), Test: isExpectedEq(int(-1))},
		{Param: math.MinInt16, Result: new(int16), Test: isExpectedEq(int16(math.MinInt16))},
		{Param: -1, Result: new(int16), Test: isExpectedEq(int16(-1))},
		{Param: 0, Result: new(int16), Test: isExpectedEq(int16(0))},
		{Param: 1, Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: math.MaxInt16, Result: new(int16), Test: isExpectedEq(int16(math.MaxInt16))},
		{Param: 1, Result: new(pgtype.Int2), Test: isExpectedEq(pgtype.Int2{Int16: 1, Valid: true})},
		{Param: "1", Result: new(string), Test: isExpectedEq("1")},
		{Param: pgtype.Int2{}, Result: new(pgtype.Int2), Test: isExpectedEq(pgtype.Int2{})},
		{Param: nil, Result: new(*int16), Test: isExpectedEq((*int16)(nil))},
	})
}

func TestInt2MarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Int2
		result string
	}{
		{source: pgtype.Int2{Int16: 0}, result: "null"},
		{source: pgtype.Int2{Int16: 1, Valid: true}, result: "1"},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestInt2UnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Int2
	}{
		{source: "null", result: pgtype.Int2{Int16: 0}},
		{source: "1", result: pgtype.Int2{Int16: 1, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Int2
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestInt4Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int4", []pgxtest.ValueRoundTripTest{
		{Param: int8(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: int16(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: int32(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: int64(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: uint8(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: uint16(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: uint32(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: uint64(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: int(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: uint(1), Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: pgtype.Int4{Int32: 1, Valid: true}, Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: int32(-1), Result: new(pgtype.Int4), Test: isExpectedEq(pgtype.Int4{Int32: -1, Valid: true})},
		{Param: 1, Result: new(int8), Test: isExpectedEq(int8(1))},
		{Param: 1, Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: 1, Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: 1, Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: 1, Result: new(uint8), Test: isExpectedEq(uint8(1))},
		{Param: 1, Result: new(uint16), Test: isExpectedEq(uint16(1))},
		{Param: 1, Result: new(uint32), Test: isExpectedEq(uint32(1))},
		{Param: 1, Result: new(uint64), Test: isExpectedEq(uint64(1))},
		{Param: 1, Result: new(int), Test: isExpectedEq(int(1))},
		{Param: 1, Result: new(uint), Test: isExpectedEq(uint(1))},
		{Param: -1, Result: new(int8), Test: isExpectedEq(int8(-1))},
		{Param: -1, Result: new(int16), Test: isExpectedEq(int16(-1))},
		{Param: -1, Result: new(int32), Test: isExpectedEq(int32(-1))},
		{Param: -1, Result: new(int64), Test: isExpectedEq(int64(-1))},
		{Param: -1, Result: new(int), Test: isExpectedEq(int(-1))},
		{Param: math.MinInt32, Result: new(int32), Test: isExpectedEq(int32(math.MinInt32))},
		{Param: -1, Result: new(int32), Test: isExpectedEq(int32(-1))},
		{Param: 0, Result: new(int32), Test: isExpectedEq(int32(0))},
		{Param: 1, Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: math.MaxInt32, Result: new(int32), Test: isExpectedEq(int32(math.MaxInt32))},
		{Param: 1, Result: new(pgtype.Int4), Test: isExpectedEq(pgtype.Int4{Int32: 1, Valid: true})},
		{Param: "1", Result: new(string), Test: isExpectedEq("1")},
		{Param: pgtype.Int4{}, Result: new(pgtype.Int4), Test: isExpectedEq(pgtype.Int4{})},
		{Param: nil, Result: new(*int32), Test: isExpectedEq((*int32)(nil))},
	})
}

func TestInt4MarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Int4
		result string
	}{
		{source: pgtype.Int4{Int32: 0}, result: "null"},
		{source: pgtype.Int4{Int32: 1, Valid: true}, result: "1"},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestInt4UnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Int4
	}{
		{source: "null", result: pgtype.Int4{Int32: 0}},
		{source: "1", result: pgtype.Int4{Int32: 1, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Int4
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestInt8Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int8", []pgxtest.ValueRoundTripTest{
		{Param: int8(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: int16(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: int32(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: int64(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: uint8(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: uint16(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: uint32(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: uint64(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: int(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: uint(1), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: pgtype.Int8{Int64: 1, Valid: true}, Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: int32(-1), Result: new(pgtype.Int8), Test: isExpectedEq(pgtype.Int8{Int64: -1, Valid: true})},
		{Param: 1, Result: new(int8), Test: isExpectedEq(int8(1))},
		{Param: 1, Result: new(int16), Test: isExpectedEq(int16(1))},
		{Param: 1, Result: new(int32), Test: isExpectedEq(int32(1))},
		{Param: 1, Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: 1, Result: new(uint8), Test: isExpectedEq(uint8(1))},
		{Param: 1, Result: new(uint16), Test: isExpectedEq(uint16(1))},
		{Param: 1, Result: new(uint32), Test: isExpectedEq(uint32(1))},
		{Param: 1, Result: new(uint64), Test: isExpectedEq(uint64(1))},
		{Param: 1, Result: new(int), Test: isExpectedEq(int(1))},
		{Param: 1, Result: new(uint), Test: isExpectedEq(uint(1))},
		{Param: -1, Result: new(int8), Test: isExpectedEq(int8(-1))},
		{Param: -1, Result: new(int16), Test: isExpectedEq(int16(-1))},
		{Param: -1, Result: new(int32), Test: isExpectedEq(int32(-1))},
		{Param: -1, Result: new(int64), Test: isExpectedEq(int64(-1))},
		{Param: -1, Result: new(int), Test: isExpectedEq(int(-1))},
		{Param: math.MinInt64, Result: new(int64), Test: isExpectedEq(int64(math.MinInt64))},
		{Param: -1, Result: new(int64), Test: isExpectedEq(int64(-1))},
		{Param: 0, Result: new(int64), Test: isExpectedEq(int64(0))},
		{Param: 1, Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: math.MaxInt64, Result: new(int64), Test: isExpectedEq(int64(math.MaxInt64))},
		{Param: 1, Result: new(pgtype.Int8), Test: isExpectedEq(pgtype.Int8{Int64: 1, Valid: true})},
		{Param: "1", Result: new(string), Test: isExpectedEq("1")},
		{Param: pgtype.Int8{}, Result: new(pgtype.Int8), Test: isExpectedEq(pgtype.Int8{})},
		{Param: nil, Result: new(*int64), Test: isExpectedEq((*int64)(nil))},
	})
}

func TestInt8MarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Int8
		result string
	}{
		{source: pgtype.Int8{Int64: 0}, result: "null"},
		{source: pgtype.Int8{Int64: 1, Valid: true}, result: "1"},
	}
	for i, tt := range successfulTests {
		r, err := tt.source.MarshalJSON()
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, string(r))
		}
	}
}

func TestInt8UnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Int8
	}{
		{source: "null", result: pgtype.Int8{Int64: 0}},
		{source: "1", result: pgtype.Int8{Int64: 1, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Int8
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
