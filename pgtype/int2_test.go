package pgtype_test

import (
	"math"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func TestInt2Transcode(t *testing.T) {
	conn := mustConnectPgx(t)
	defer mustClose(t, conn)

	ps, err := conn.Prepare("test", "select $1::int2")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		result pgtype.Int2
	}{
		{result: pgtype.Int2{Int: math.MinInt16, Status: pgtype.Present}},
		{result: pgtype.Int2{Int: -1, Status: pgtype.Present}},
		{result: pgtype.Int2{Int: 0, Status: pgtype.Present}},
		{result: pgtype.Int2{Int: 1, Status: pgtype.Present}},
		{result: pgtype.Int2{Int: math.MaxInt16, Status: pgtype.Present}},
	}

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for _, fc := range formats {
		ps.FieldDescriptions[0].FormatCode = fc.formatCode
		for i, tt := range tests {
			var r pgtype.Int2
			err := conn.QueryRow("test", tt.result).Scan(&r)
			if err != nil {
				t.Errorf("%v %d: %v", fc.name, i, err)
			}

			if r != tt.result {
				t.Errorf("%v %d: expected %v, got %v", fc.name, i, tt.result, r)
			}
		}
	}
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
	}
}
