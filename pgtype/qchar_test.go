package pgtype_test

import (
	"context"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestQcharTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support qchar")

	var tests []pgxtest.ValueRoundTripTest
	for i := 0; i <= math.MaxUint8; i++ {
		tests = append(tests, pgxtest.ValueRoundTripTest{Param: rune(i), Result: new(rune), Test: isExpectedEq(rune(i))})
		tests = append(tests, pgxtest.ValueRoundTripTest{Param: byte(i), Result: new(byte), Test: isExpectedEq(byte(i))})
	}
	tests = append(tests, pgxtest.ValueRoundTripTest{Param: nil, Result: new(*rune), Test: isExpectedEq((*rune)(nil))})
	tests = append(tests, pgxtest.ValueRoundTripTest{Param: nil, Result: new(*byte), Test: isExpectedEq((*byte)(nil))})

	// Can only test with known OIDs as rune and byte would be considered numbers.
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, `"char"`, tests)
}

// TestQcharCodecPlanScanString is a regression test for https://github.com/jackc/pgx/issues/2314.
//
// Scanning a "char" column (OID 18) into *string used to succeed in TextFormat but
// fail in BinaryFormat with "cannot scan char (OID 18) in binary format into *string".
// Both formats must now produce the same result.
func TestQcharCodecPlanScanString(t *testing.T) {
	m := pgtype.NewMap()

	for _, tt := range []struct {
		name   string
		format int16
	}{
		{"text", pgtype.TextFormatCode},
		{"binary", pgtype.BinaryFormatCode},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var s string
			plan := m.PlanScan(pgtype.QCharOID, tt.format, &s)
			if plan == nil {
				t.Fatalf("PlanScan returned nil plan for *string in %s format", tt.name)
			}
			if err := plan.Scan([]byte{'a'}, &s); err != nil {
				t.Fatalf("Scan failed in %s format: %v", tt.name, err)
			}
			if s != "a" {
				t.Fatalf("Scan result mismatch in %s format: got %q want %q", tt.name, s, "a")
			}
		})
	}

	// Empty src must produce empty string (mirrors *byte / *rune zero-value behavior).
	t.Run("empty-binary", func(t *testing.T) {
		var s string = "x"
		plan := m.PlanScan(pgtype.QCharOID, pgtype.BinaryFormatCode, &s)
		if err := plan.Scan([]byte{}, &s); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if s != "" {
			t.Fatalf("empty src: got %q want %q", s, "")
		}
	})

	// 0xC8 (200): a byte >= 128. Both formats must yield the raw 1-byte string
	// "\xc8", not the 2-byte UTF-8 encoding "\xc3\x88". This is the case that
	// catches string(src[0]) UTF-8-encoding the byte instead of copying it.
	t.Run("non-ascii-byte", func(t *testing.T) {
		for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
			var s string
			plan := m.PlanScan(pgtype.QCharOID, format, &s)
			if err := plan.Scan([]byte{0xC8}, &s); err != nil {
				t.Fatalf("format %d: scan failed: %v", format, err)
			}
			if s != "\xc8" {
				t.Fatalf("format %d: got %q (% x) want %q", format, s, s, "\xc8")
			}
		}
	})

	// Multi-byte src is an invalid "char" payload and must error.
	t.Run("too-long", func(t *testing.T) {
		var s string
		plan := m.PlanScan(pgtype.QCharOID, pgtype.BinaryFormatCode, &s)
		if err := plan.Scan([]byte("ab"), &s); err == nil {
			t.Fatalf("expected error for 2-byte src, got nil (s=%q)", s)
		}
	})
}
