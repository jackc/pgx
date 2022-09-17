// Do not edit. Generated from pgtype/int_test.go.erb
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
		{int8(1), new(int16), isExpectedEq(int16(1))},
		{int16(1), new(int16), isExpectedEq(int16(1))},
		{int32(1), new(int16), isExpectedEq(int16(1))},
		{int64(1), new(int16), isExpectedEq(int16(1))},
		{uint8(1), new(int16), isExpectedEq(int16(1))},
		{uint16(1), new(int16), isExpectedEq(int16(1))},
		{uint32(1), new(int16), isExpectedEq(int16(1))},
		{uint64(1), new(int16), isExpectedEq(int16(1))},
		{int(1), new(int16), isExpectedEq(int16(1))},
		{uint(1), new(int16), isExpectedEq(int16(1))},
		{pgtype.Int2{Int16: 1, Valid: true}, new(int16), isExpectedEq(int16(1))},
		{int32(-1), new(pgtype.Int2), isExpectedEq(pgtype.Int2{Int16: -1, Valid: true})},
		{1, new(int8), isExpectedEq(int8(1))},
		{1, new(int16), isExpectedEq(int16(1))},
		{1, new(int32), isExpectedEq(int32(1))},
		{1, new(int64), isExpectedEq(int64(1))},
		{1, new(uint8), isExpectedEq(uint8(1))},
		{1, new(uint16), isExpectedEq(uint16(1))},
		{1, new(uint32), isExpectedEq(uint32(1))},
		{1, new(uint64), isExpectedEq(uint64(1))},
		{1, new(int), isExpectedEq(int(1))},
		{1, new(uint), isExpectedEq(uint(1))},
		{-1, new(int8), isExpectedEq(int8(-1))},
		{-1, new(int16), isExpectedEq(int16(-1))},
		{-1, new(int32), isExpectedEq(int32(-1))},
		{-1, new(int64), isExpectedEq(int64(-1))},
		{-1, new(int), isExpectedEq(int(-1))},
		{math.MinInt16, new(int16), isExpectedEq(int16(math.MinInt16))},
		{-1, new(int16), isExpectedEq(int16(-1))},
		{0, new(int16), isExpectedEq(int16(0))},
		{1, new(int16), isExpectedEq(int16(1))},
		{math.MaxInt16, new(int16), isExpectedEq(int16(math.MaxInt16))},
		{1, new(pgtype.Int2), isExpectedEq(pgtype.Int2{Int16: 1, Valid: true})},
		{"1", new(string), isExpectedEq("1")},
		{pgtype.Int2{}, new(pgtype.Int2), isExpectedEq(pgtype.Int2{})},
		{nil, new(*int16), isExpectedEq((*int16)(nil))},
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
		{int8(1), new(int32), isExpectedEq(int32(1))},
		{int16(1), new(int32), isExpectedEq(int32(1))},
		{int32(1), new(int32), isExpectedEq(int32(1))},
		{int64(1), new(int32), isExpectedEq(int32(1))},
		{uint8(1), new(int32), isExpectedEq(int32(1))},
		{uint16(1), new(int32), isExpectedEq(int32(1))},
		{uint32(1), new(int32), isExpectedEq(int32(1))},
		{uint64(1), new(int32), isExpectedEq(int32(1))},
		{int(1), new(int32), isExpectedEq(int32(1))},
		{uint(1), new(int32), isExpectedEq(int32(1))},
		{pgtype.Int4{Int32: 1, Valid: true}, new(int32), isExpectedEq(int32(1))},
		{int32(-1), new(pgtype.Int4), isExpectedEq(pgtype.Int4{Int32: -1, Valid: true})},
		{1, new(int8), isExpectedEq(int8(1))},
		{1, new(int16), isExpectedEq(int16(1))},
		{1, new(int32), isExpectedEq(int32(1))},
		{1, new(int64), isExpectedEq(int64(1))},
		{1, new(uint8), isExpectedEq(uint8(1))},
		{1, new(uint16), isExpectedEq(uint16(1))},
		{1, new(uint32), isExpectedEq(uint32(1))},
		{1, new(uint64), isExpectedEq(uint64(1))},
		{1, new(int), isExpectedEq(int(1))},
		{1, new(uint), isExpectedEq(uint(1))},
		{-1, new(int8), isExpectedEq(int8(-1))},
		{-1, new(int16), isExpectedEq(int16(-1))},
		{-1, new(int32), isExpectedEq(int32(-1))},
		{-1, new(int64), isExpectedEq(int64(-1))},
		{-1, new(int), isExpectedEq(int(-1))},
		{math.MinInt32, new(int32), isExpectedEq(int32(math.MinInt32))},
		{-1, new(int32), isExpectedEq(int32(-1))},
		{0, new(int32), isExpectedEq(int32(0))},
		{1, new(int32), isExpectedEq(int32(1))},
		{math.MaxInt32, new(int32), isExpectedEq(int32(math.MaxInt32))},
		{1, new(pgtype.Int4), isExpectedEq(pgtype.Int4{Int32: 1, Valid: true})},
		{"1", new(string), isExpectedEq("1")},
		{pgtype.Int4{}, new(pgtype.Int4), isExpectedEq(pgtype.Int4{})},
		{nil, new(*int32), isExpectedEq((*int32)(nil))},
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
		{int8(1), new(int64), isExpectedEq(int64(1))},
		{int16(1), new(int64), isExpectedEq(int64(1))},
		{int32(1), new(int64), isExpectedEq(int64(1))},
		{int64(1), new(int64), isExpectedEq(int64(1))},
		{uint8(1), new(int64), isExpectedEq(int64(1))},
		{uint16(1), new(int64), isExpectedEq(int64(1))},
		{uint32(1), new(int64), isExpectedEq(int64(1))},
		{uint64(1), new(int64), isExpectedEq(int64(1))},
		{int(1), new(int64), isExpectedEq(int64(1))},
		{uint(1), new(int64), isExpectedEq(int64(1))},
		{pgtype.Int8{Int64: 1, Valid: true}, new(int64), isExpectedEq(int64(1))},
		{int32(-1), new(pgtype.Int8), isExpectedEq(pgtype.Int8{Int64: -1, Valid: true})},
		{1, new(int8), isExpectedEq(int8(1))},
		{1, new(int16), isExpectedEq(int16(1))},
		{1, new(int32), isExpectedEq(int32(1))},
		{1, new(int64), isExpectedEq(int64(1))},
		{1, new(uint8), isExpectedEq(uint8(1))},
		{1, new(uint16), isExpectedEq(uint16(1))},
		{1, new(uint32), isExpectedEq(uint32(1))},
		{1, new(uint64), isExpectedEq(uint64(1))},
		{1, new(int), isExpectedEq(int(1))},
		{1, new(uint), isExpectedEq(uint(1))},
		{-1, new(int8), isExpectedEq(int8(-1))},
		{-1, new(int16), isExpectedEq(int16(-1))},
		{-1, new(int32), isExpectedEq(int32(-1))},
		{-1, new(int64), isExpectedEq(int64(-1))},
		{-1, new(int), isExpectedEq(int(-1))},
		{math.MinInt64, new(int64), isExpectedEq(int64(math.MinInt64))},
		{-1, new(int64), isExpectedEq(int64(-1))},
		{0, new(int64), isExpectedEq(int64(0))},
		{1, new(int64), isExpectedEq(int64(1))},
		{math.MaxInt64, new(int64), isExpectedEq(int64(math.MaxInt64))},
		{1, new(pgtype.Int8), isExpectedEq(pgtype.Int8{Int64: 1, Valid: true})},
		{"1", new(string), isExpectedEq("1")},
		{pgtype.Int8{}, new(pgtype.Int8), isExpectedEq(pgtype.Int8{})},
		{nil, new(*int64), isExpectedEq((*int64)(nil))},
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
