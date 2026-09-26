package pgtype_test

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNumericCodecsEncodeText(t *testing.T) {
	typeMap := pgtype.NewMap()
	int4Type, ok := typeMap.TypeForOID(pgtype.Int4OID)
	if !ok {
		t.Fatal("int4 type is not registered")
	}

	tests := []struct {
		name  string
		codec pgtype.Codec
		oid   uint32
		value any
		want  string
	}{
		{name: "int2", codec: pgtype.Int2Codec{}, oid: pgtype.Int2OID, value: int16(-30_000), want: "-30000"},
		{name: "int2 valuer", codec: pgtype.Int2Codec{}, oid: pgtype.Int2OID, value: pgtype.Int2{Int16: -30_000, Valid: true}, want: "-30000"},
		{name: "int4", codec: pgtype.Int4Codec{}, oid: pgtype.Int4OID, value: int32(35_000_000), want: "35000000"},
		{name: "int4 valuer", codec: pgtype.Int4Codec{}, oid: pgtype.Int4OID, value: pgtype.Int4{Int32: 35_000_000, Valid: true}, want: "35000000"},
		{name: "int8", codec: pgtype.Int8Codec{}, oid: pgtype.Int8OID, value: int64(math.MinInt64), want: "-9223372036854775808"},
		{name: "int8 valuer", codec: pgtype.Int8Codec{}, oid: pgtype.Int8OID, value: pgtype.Int8{Int64: math.MaxInt64, Valid: true}, want: "9223372036854775807"},
		{name: "uint32", codec: pgtype.Uint32Codec{}, value: uint32(math.MaxUint32), want: "4294967295"},
		{name: "uint32 int64 valuer", codec: pgtype.Uint32Codec{}, value: pgtype.Int8{Int64: 35_000_000, Valid: true}, want: "35000000"},
		{name: "uint64", codec: pgtype.Uint64Codec{}, value: uint64(math.MaxUint64), want: "18446744073709551615"},
		{name: "uint64 int64 valuer", codec: pgtype.Uint64Codec{}, value: pgtype.Int8{Int64: 35_000_000, Valid: true}, want: "35000000"},
		{name: "float4", codec: pgtype.Float4Codec{}, oid: pgtype.Float4OID, value: float32(12_345.25), want: "12345.25"},
		{name: "float4 valuer", codec: pgtype.Float4Codec{}, oid: pgtype.Float4OID, value: pgtype.Float4{Float32: 12_345.25, Valid: true}, want: "12345.25"},
		{name: "float8", codec: pgtype.Float8Codec{}, oid: pgtype.Float8OID, value: float64(12_345.25), want: "12345.25"},
		{name: "float8 valuer", codec: pgtype.Float8Codec{}, oid: pgtype.Float8OID, value: pgtype.Float8{Float64: 12_345.25, Valid: true}, want: "12345.25"},
		{name: "float8 int64 valuer", codec: pgtype.Float8Codec{}, oid: pgtype.Float8OID, value: pgtype.Int8{Int64: 35_000_000, Valid: true}, want: "35000000"},
		{name: "numeric int64", codec: pgtype.NumericCodec{}, oid: pgtype.NumericOID, value: pgtype.Int8{Int64: 35_000_000, Valid: true}, want: "35000000"},
		{name: "numeric float64", codec: pgtype.NumericCodec{}, oid: pgtype.NumericOID, value: pgtype.Float8{Float64: 12_345.25, Valid: true}, want: "12345.25"},
		{name: "interval", codec: pgtype.IntervalCodec{}, oid: pgtype.IntervalOID, value: pgtype.Interval{Months: 1_200, Days: 3_400, Microseconds: 56_000_000, Valid: true}, want: "1200 mon 3400 day 00:00:56"},
		{
			name:  "array dimensions",
			codec: &pgtype.ArrayCodec{ElementType: int4Type},
			oid:   pgtype.Int4ArrayOID,
			value: pgtype.Array[int32]{
				Elements: []int32{35_000_000},
				Dims:     []pgtype.ArrayDimension{{Length: 1, LowerBound: 35_000}},
				Valid:    true,
			},
			want: "[35000:35000]={35000000}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := tt.codec.PlanEncode(typeMap, tt.oid, pgtype.TextFormatCode, tt.value)
			if plan == nil {
				t.Fatal("no encode plan")
			}

			got, err := plan.Encode(tt.value, []byte("prefix:"))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "prefix:"+tt.want {
				t.Fatalf("encoded value = %q, want %q", got, "prefix:"+tt.want)
			}
		})
	}
}

func BenchmarkPrimitiveNumericCodecsEncodeText(b *testing.B) {
	tests := []struct {
		name  string
		codec pgtype.Codec
		value any
	}{
		{name: "int16", codec: pgtype.Int2Codec{}, value: int16(30_000)},
		{name: "int32", codec: pgtype.Int4Codec{}, value: int32(35_000_000)},
		{name: "int64", codec: pgtype.Int8Codec{}, value: int64(35_000_000)},
		{name: "uint32", codec: pgtype.Uint32Codec{}, value: uint32(35_000_000)},
		{name: "uint64", codec: pgtype.Uint64Codec{}, value: uint64(35_000_000)},
		{name: "float32", codec: pgtype.Float4Codec{}, value: float32(35_000_000.25)},
		{name: "float64", codec: pgtype.Float8Codec{}, value: float64(35_000_000.25)},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			plan := tt.codec.PlanEncode(pgtype.NewMap(), 0, pgtype.TextFormatCode, tt.value)
			buf := make([]byte, 0, 32)

			b.ReportAllocs()
			for b.Loop() {
				encoded, err := plan.Encode(tt.value, buf[:0])
				if err != nil {
					b.Fatal(err)
				}
				if len(encoded) == 0 {
					b.Fatal("encoded value is empty")
				}
			}
		})
	}
}

func BenchmarkCompositeNumericCodecsEncodeText(b *testing.B) {
	typeMap := pgtype.NewMap()
	int4Type, ok := typeMap.TypeForOID(pgtype.Int4OID)
	if !ok {
		b.Fatal("int4 type is not registered")
	}

	tests := []struct {
		name  string
		codec pgtype.Codec
		oid   uint32
		value any
	}{
		{
			name:  "interval",
			codec: pgtype.IntervalCodec{},
			oid:   pgtype.IntervalOID,
			value: pgtype.Interval{Months: 1_200, Days: 3_400, Microseconds: 56_000_000, Valid: true},
		},
		{
			name:  "numeric/int64",
			codec: pgtype.NumericCodec{},
			oid:   pgtype.NumericOID,
			value: pgtype.Int8{Int64: 35_000_000, Valid: true},
		},
		{
			name:  "numeric/float64",
			codec: pgtype.NumericCodec{},
			oid:   pgtype.NumericOID,
			value: pgtype.Float8{Float64: 35_000_000.25, Valid: true},
		},
		{
			name:  "array/dimensions",
			codec: &pgtype.ArrayCodec{ElementType: int4Type},
			oid:   pgtype.Int4ArrayOID,
			value: pgtype.Array[int32]{
				Elements: []int32{35_000_000},
				Dims:     []pgtype.ArrayDimension{{Length: 1, LowerBound: 35_000}},
				Valid:    true,
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			plan := tt.codec.PlanEncode(typeMap, tt.oid, pgtype.TextFormatCode, tt.value)
			buf := make([]byte, 0, 64)

			b.ReportAllocs()
			for b.Loop() {
				encoded, err := plan.Encode(tt.value, buf[:0])
				if err != nil {
					b.Fatal(err)
				}
				if len(encoded) == 0 {
					b.Fatal("encoded value is empty")
				}
			}
		})
	}
}
