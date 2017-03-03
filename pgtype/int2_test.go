package pgtype_test

import (
	"math"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestInt2Transcode(t *testing.T) {
	testSuccessfulTranscode(t, "int2", []interface{}{
		pgtype.Int2{Int: math.MinInt16, Status: pgtype.Present},
		pgtype.Int2{Int: -1, Status: pgtype.Present},
		pgtype.Int2{Int: 0, Status: pgtype.Present},
		pgtype.Int2{Int: 1, Status: pgtype.Present},
		pgtype.Int2{Int: math.MaxInt16, Status: pgtype.Present},
		pgtype.Int2{Int: 0, Status: pgtype.Null},
	})
}

func TestInt2ConvertFrom(t *testing.T) {
	type _int8 int8

	successfulTests := []struct {
		source interface{}
		result pgtype.Int2
	}{
		{source: int8(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: int16(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: int32(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: int64(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: int8(-1), result: pgtype.Int2{Int: -1, Status: pgtype.Present}},
		{source: int16(-1), result: pgtype.Int2{Int: -1, Status: pgtype.Present}},
		{source: int32(-1), result: pgtype.Int2{Int: -1, Status: pgtype.Present}},
		{source: int64(-1), result: pgtype.Int2{Int: -1, Status: pgtype.Present}},
		{source: uint8(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: uint16(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: uint32(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: uint64(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: "1", result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{source: _int8(1), result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Int2
		err := r.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
