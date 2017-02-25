package pgtype_test

import (
	"math"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestInt4Transcode(t *testing.T) {
	testSuccessfulTranscode(t, "int4", []interface{}{
		pgtype.Int4{Int: math.MinInt32, Status: pgtype.Present},
		pgtype.Int4{Int: -1, Status: pgtype.Present},
		pgtype.Int4{Int: 0, Status: pgtype.Present},
		pgtype.Int4{Int: 1, Status: pgtype.Present},
		pgtype.Int4{Int: math.MaxInt32, Status: pgtype.Present},
		pgtype.Int4{Int: 0, Status: pgtype.Null},
	})
}

func TestInt4ConvertFrom(t *testing.T) {
	type _int8 int8

	successfulTests := []struct {
		source interface{}
		result pgtype.Int4
	}{
		{source: int8(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: int16(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: int32(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: int64(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: int8(-1), result: pgtype.Int4{Int: -1, Status: pgtype.Present}},
		{source: int16(-1), result: pgtype.Int4{Int: -1, Status: pgtype.Present}},
		{source: int32(-1), result: pgtype.Int4{Int: -1, Status: pgtype.Present}},
		{source: int64(-1), result: pgtype.Int4{Int: -1, Status: pgtype.Present}},
		{source: uint8(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: uint16(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: uint32(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: uint64(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: "1", result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
		{source: _int8(1), result: pgtype.Int4{Int: 1, Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Int4
		err := r.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
}
