package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func BenchmarkDateCodecScanText(b *testing.B) {
	c := pgtype.DateCodec{}
	var d pgtype.Date
	plan := c.PlanScan(nil, pgtype.DateOID, pgtype.TextFormatCode, &d)
	src := []byte("2024-01-02")

	b.ReportAllocs()
	for b.Loop() {
		if err := plan.Scan(src, &d); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTimestampCodecScanText(b *testing.B) {
	c := &pgtype.TimestampCodec{}
	var ts pgtype.Timestamp
	plan := c.PlanScan(nil, pgtype.TimestampOID, pgtype.TextFormatCode, &ts)
	src := []byte("2024-01-02 03:04:05.123456")

	b.ReportAllocs()
	for b.Loop() {
		if err := plan.Scan(src, &ts); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTimestamptzCodecScanText(b *testing.B) {
	c := &pgtype.TimestamptzCodec{}
	var tstz pgtype.Timestamptz
	plan := c.PlanScan(nil, pgtype.TimestamptzOID, pgtype.TextFormatCode, &tstz)
	src := []byte("2024-01-02 03:04:05.123456+00")

	b.ReportAllocs()
	for b.Loop() {
		if err := plan.Scan(src, &tstz); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTimestampCodecEncodeText(b *testing.B) {
	m := pgtype.NewMap()
	src := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)
	buf := make([]byte, 0, 64)

	b.ReportAllocs()
	for b.Loop() {
		var err error
		buf, err = m.Encode(pgtype.TimestampOID, pgtype.TextFormatCode, src, buf[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
}
