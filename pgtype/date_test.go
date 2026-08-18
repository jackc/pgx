package pgtype_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isExpectedEqTime(a any) func(any) bool {
	return func(v any) bool {
		at := a.(time.Time)
		vt := v.(time.Time)

		return at.Equal(vt)
	}
}

func TestDateCodec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "date", []pgxtest.ValueRoundTripTest{
		{Param: time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC))},
		{Param: time.Date(12200, 1, 2, 0, 0, 0, 0, time.UTC), Result: new(time.Time), Test: isExpectedEqTime(time.Date(12200, 1, 2, 0, 0, 0, 0, time.UTC))},
		{Param: pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true}, Result: new(pgtype.Date), Test: isExpectedEq(pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true})},
		{Param: pgtype.Date{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, Result: new(pgtype.Date), Test: isExpectedEq(pgtype.Date{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{Param: pgtype.Date{}, Result: new(pgtype.Date), Test: isExpectedEq(pgtype.Date{})},
		{Param: nil, Result: new(*time.Time), Test: isExpectedEq((*time.Time)(nil))},
	})
}

func TestDateCodecTextEncode(t *testing.T) {
	m := pgtype.NewMap()

	successfulTests := []struct {
		source pgtype.Date
		result string
	}{
		{source: pgtype.Date{Time: time.Date(2012, 3, 29, 0, 0, 0, 0, time.UTC), Valid: true}, result: "2012-03-29"},
		{source: pgtype.Date{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Valid: true}, result: "2012-03-29"},
		{source: pgtype.Date{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Valid: true}, result: "2012-03-29"},
		{source: pgtype.Date{Time: time.Date(789, 1, 2, 0, 0, 0, 0, time.UTC), Valid: true}, result: "0789-01-02"},
		{source: pgtype.Date{Time: time.Date(89, 1, 2, 0, 0, 0, 0, time.UTC), Valid: true}, result: "0089-01-02"},
		{source: pgtype.Date{Time: time.Date(9, 1, 2, 0, 0, 0, 0, time.UTC), Valid: true}, result: "0009-01-02"},
		{source: pgtype.Date{Time: time.Date(12200, 1, 2, 0, 0, 0, 0, time.UTC), Valid: true}, result: "12200-01-02"},
		{source: pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true}, result: "infinity"},
		{source: pgtype.Date{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, result: "-infinity"},
	}
	for i, tt := range successfulTests {
		buf, err := m.Encode(pgtype.DateOID, pgtype.TextFormatCode, tt.source, nil)
		assert.NoErrorf(t, err, "%d", i)
		assert.Equalf(t, tt.result, string(buf), "%d", i)
	}
}

func TestDateMarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source pgtype.Date
		result string
	}{
		{source: pgtype.Date{}, result: "null"},
		{source: pgtype.Date{Time: time.Date(2012, 3, 29, 0, 0, 0, 0, time.UTC), Valid: true}, result: "\"2012-03-29\""},
		{source: pgtype.Date{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Valid: true}, result: "\"2012-03-29\""},
		{source: pgtype.Date{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Valid: true}, result: "\"2012-03-29\""},
		{source: pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true}, result: "\"infinity\""},
		{source: pgtype.Date{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, result: "\"-infinity\""},
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

func TestDateUnmarshalJSON(t *testing.T) {
	successfulTests := []struct {
		source string
		result pgtype.Date
	}{
		{source: "null", result: pgtype.Date{}},
		{source: "\"2012-03-29\"", result: pgtype.Date{Time: time.Date(2012, 3, 29, 0, 0, 0, 0, time.UTC), Valid: true}},
		{source: "\"2012-03-29\"", result: pgtype.Date{Time: time.Date(2012, 3, 29, 10, 5, 45, 0, time.FixedZone("", -6*60*60)), Valid: true}},
		{source: "\"2012-03-29\"", result: pgtype.Date{Time: time.Date(2012, 3, 29, 10, 5, 45, 555*1000*1000, time.FixedZone("", -6*60*60)), Valid: true}},
		{source: "\"infinity\"", result: pgtype.Date{InfinityModifier: pgtype.Infinity, Valid: true}},
		{source: "\"-infinity\"", result: pgtype.Date{InfinityModifier: pgtype.NegativeInfinity, Valid: true}},
	}
	for i, tt := range successfulTests {
		var r pgtype.Date
		err := r.UnmarshalJSON([]byte(tt.source))
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r.Time.Year() != tt.result.Time.Year() || r.Time.Month() != tt.result.Time.Month() || r.Time.Day() != tt.result.Time.Day() || r.Valid != tt.result.Valid || r.InfinityModifier != tt.result.InfinityModifier {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestDateScanTextFormat(t *testing.T) {
	// Tests for scanPlanTextAnyToDateScanner

	t.Run("StandardDates", func(t *testing.T) {
		tests := []struct {
			input string
			year  int
			month time.Month
			day   int
		}{
			// Typical dates
			{"2024-01-15", 2024, 1, 15},
			{"2024-12-31", 2024, 12, 31},
			{"1999-06-15", 1999, 6, 15},
			{"2000-01-01", 2000, 1, 1},
			{"2000-01-02", 2000, 1, 2},

			// Epoch boundaries
			{"1970-01-01", 1970, 1, 1},
			{"1969-12-31", 1969, 12, 31},

			// Y2K boundaries
			{"1999-12-31", 1999, 12, 31},

			// Leap year dates
			{"2000-02-29", 2000, 2, 29},
			{"2024-02-29", 2024, 2, 29},
			{"1900-02-28", 1900, 2, 28},

			// Month boundaries
			{"2024-01-31", 2024, 1, 31},
			{"2024-04-30", 2024, 4, 30},
			{"2024-06-30", 2024, 6, 30},

			// Old dates
			{"1900-01-01", 1900, 1, 1},
			{"1800-06-15", 1800, 6, 15},
			{"1000-01-01", 1000, 1, 1},

			// Future dates
			{"2100-01-01", 2100, 1, 1},
			{"2200-12-31", 2200, 12, 31},
			{"3000-06-15", 3000, 6, 15},

			// 5+ digit years
			{"12200-01-02", 12200, 1, 2},
			{"99999-12-31", 99999, 12, 31},
			{"100000-01-01", 100000, 1, 1},

			// Zero-padded years
			{"0001-01-01", 1, 1, 1},
			{"0009-01-02", 9, 1, 2},
			{"0089-01-02", 89, 1, 2},
			{"0789-01-02", 789, 1, 2},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				var d pgtype.Date
				err := d.Scan(tt.input)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !d.Valid {
					t.Error("expected Valid=true")
				}
				if d.Time.Year() != tt.year {
					t.Errorf("year: got %d, want %d", d.Time.Year(), tt.year)
				}
				if d.Time.Month() != tt.month {
					t.Errorf("month: got %d, want %d", d.Time.Month(), tt.month)
				}
				if d.Time.Day() != tt.day {
					t.Errorf("day: got %d, want %d", d.Time.Day(), tt.day)
				}
			})
		}
	})

	t.Run("BCDates", func(t *testing.T) {
		tests := []struct {
			input string
			year  int
			month time.Month
			day   int
		}{
			// BC date handling: year X BC = Go year (-X + 1)
			// 1 BC = year 0, 2 BC = year -1, etc.
			{"0001-01-01 BC", 0, 1, 1},
			{"0002-01-01 BC", -1, 1, 1},
			{"0100-06-15 BC", -99, 6, 15},
			{"0500-12-31 BC", -499, 12, 31},
			{"1000-01-01 BC", -999, 1, 1},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				var d pgtype.Date
				err := d.Scan(tt.input)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !d.Valid {
					t.Error("expected Valid=true")
				}
				if d.Time.Year() != tt.year {
					t.Errorf("year: got %d, want %d", d.Time.Year(), tt.year)
				}
				if d.Time.Month() != tt.month {
					t.Errorf("month: got %d, want %d", d.Time.Month(), tt.month)
				}
				if d.Time.Day() != tt.day {
					t.Errorf("day: got %d, want %d", d.Time.Day(), tt.day)
				}
			})
		}
	})

	t.Run("Infinity", func(t *testing.T) {
		tests := []struct {
			input   string
			wantMod pgtype.InfinityModifier
		}{
			{"infinity", pgtype.Infinity},
			{"-infinity", pgtype.NegativeInfinity},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				var d pgtype.Date
				err := d.Scan(tt.input)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !d.Valid {
					t.Error("expected Valid=true")
				}
				if d.InfinityModifier != tt.wantMod {
					t.Errorf("got InfinityModifier=%d, want %d", d.InfinityModifier, tt.wantMod)
				}
			})
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		tests := []string{
			// Too short
			"",
			"2024",
			"2024-01",
			"2024-1-1",

			// Wrong separators
			"2024/01/15",
			"2024.01.15",
			"20240115",

			// Invalid characters
			"2024-0a-15",
			"2024-01-1b",
			"abcd-01-15",

			// Wrong format
			"24-01-15",
			"01-15-2024",
			"15-01-2024",

			// Trailing garbage
			"2024-01-15x",
			"2024-01-15 ",
			"2024-01-15\n",

			// Leading garbage
			" 2024-01-15",
			"x2024-01-15",

			// Wrong month/day digits
			"2024-1-15",
			"2024-01-5",
			"2024-001-15",
			"2024-01-015",

			// Partial infinity
			"infinit",
			"infinityy",
			"-infinit",
			"--infinity",
			"Infinity",
			"-Infinity",
			"INFINITY",

			// Malformed BC
			"2024-01-15BC",
			"2024-01-15 bc",
			"2024-01-15  BC",
			"2024-01-15 B",

			// Only 3 digit year
			"123-01-15",
		}

		for _, tt := range tests {
			t.Run(tt, func(t *testing.T) {
				var d pgtype.Date
				err := d.Scan(tt)
				if err == nil {
					t.Errorf("expected error for input %q, got date %+v", tt, d)
				}
			})
		}
	})

	t.Run("Nil", func(t *testing.T) {
		var d pgtype.Date
		err := d.Scan(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.Valid {
			t.Error("expected Valid=false for nil input")
		}
	})
}

func TestDateScanRoundTrip(t *testing.T) {
	// Test that dates from TestDateCodec roundtrip correctly through text format
	dates := []time.Time{
		time.Date(-100, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(12200, 1, 2, 0, 0, 0, 0, time.UTC),
	}

	m := pgtype.NewMap()

	for _, d := range dates {
		t.Run(d.Format("2006-01-02"), func(t *testing.T) {
			src := pgtype.Date{Time: d, Valid: true}

			// Encode to text
			buf, err := m.Encode(pgtype.DateOID, pgtype.TextFormatCode, src, nil)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			// Decode back
			var dst pgtype.Date
			err = dst.Scan(string(buf))
			if err != nil {
				t.Fatalf("scan failed for %q: %v", string(buf), err)
			}

			if !dst.Valid {
				t.Error("expected Valid=true")
			}
			if dst.Time.Year() != d.Year() || dst.Time.Month() != d.Month() || dst.Time.Day() != d.Day() {
				t.Errorf("roundtrip failed: input=%v encoded=%q parsed=%v", d, string(buf), dst.Time)
			}
		})
	}
}

func TestDateScanInfinityRoundTrip(t *testing.T) {
	m := pgtype.NewMap()

	tests := []pgtype.Date{
		{InfinityModifier: pgtype.Infinity, Valid: true},
		{InfinityModifier: pgtype.NegativeInfinity, Valid: true},
	}

	for _, src := range tests {
		buf, err := m.Encode(pgtype.DateOID, pgtype.TextFormatCode, src, nil)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}

		var dst pgtype.Date
		err = dst.Scan(string(buf))
		if err != nil {
			t.Fatalf("scan failed for %q: %v", string(buf), err)
		}

		if dst != src {
			t.Errorf("roundtrip failed: src=%+v dst=%+v", src, dst)
		}
	}
}

func TestDateCodecDecodeText(t *testing.T) {
	c := pgtype.DateCodec{}

	for _, tt := range []struct {
		src  string
		want time.Time
	}{
		{`2024-01-02`, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{`0001-01-01`, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{`10000-01-02`, time.Date(10000, 1, 2, 0, 0, 0, 0, time.UTC)},

		// date reaches much further than timestamp does: JULIAN_MAXYEAR is 5874898, so the
		// last representable date is the end of 5874897.
		{`5874897-12-31`, time.Date(5874897, 12, 31, 0, 0, 0, 0, time.UTC)},

		{`0001-01-01 BC`, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{`4713-02-29 BC`, time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC)},
		{`4714-11-24 BC`, time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
	} {
		var d pgtype.Date
		plan := c.PlanScan(nil, pgtype.DateOID, pgtype.TextFormatCode, &d)

		err := plan.Scan([]byte(tt.src), &d)
		require.NoErrorf(t, err, "%s", tt.src)
		require.Truef(t, d.Valid, "%s", tt.src)
		require.Equalf(t, tt.want, d.Time, "%s", tt.src)
	}
}

func TestDateCodecDecodeTextInvalid(t *testing.T) {
	c := pgtype.DateCodec{}

	for _, src := range []string{
		`eeeee`,
		`0000-01-01`,

		// Impossible dates used to be normalized by time.Date rather than rejected, so
		// 2024-02-30 came back as 2024-03-01 and 2024-13-01 as 2025-01-01.
		`2024-02-30`,
		`2024-13-01`,
		`2024-00-01`,
		`2023-02-29`,
		`4712-02-29 BC`,

		// Outside PostgreSQL's range for date.
		`5874898-01-01`,
		`4714-11-23 BC`,

		// A time of day makes it a timestamp, not a date.
		`2024-01-02 03:04:05`,
	} {
		var d pgtype.Date
		plan := c.PlanScan(nil, pgtype.DateOID, pgtype.TextFormatCode, &d)
		err := plan.Scan([]byte(src), &d)
		require.Errorf(t, err, "%s", src)
	}
}

func TestDateCodecEncodeText(t *testing.T) {
	m := pgtype.NewMap()

	for _, tt := range []struct {
		src  time.Time
		want string
	}{
		{time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), `2024-01-02`},
		{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), `0001-01-01`},
		{time.Date(10000, 1, 2, 0, 0, 0, 0, time.UTC), `10000-01-02`},
		{time.Date(5874897, 12, 31, 0, 0, 0, 0, time.UTC), `5874897-12-31`},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), `0001-01-01 BC`},
		{time.Date(-4712, 2, 29, 0, 0, 0, 0, time.UTC), `4713-02-29 BC`},
	} {
		buf, err := m.Encode(pgtype.DateOID, pgtype.TextFormatCode, tt.src, nil)
		require.NoErrorf(t, err, "%v", tt.src)
		require.Equalf(t, tt.want, string(buf), "%v", tt.src)
	}
}

// TestDateCodecScanBinaryRange pins the range limits on the binary scan path. PostgreSQL
// never sends a value outside them, but the text path rejects them, so the binary path has
// to as well -- otherwise whether a value is accepted would depend on QueryExecMode.
func TestDateCodecScanBinaryRange(t *testing.T) {
	c := pgtype.DateCodec{}

	// The day offsets of PostgreSQL's first and last representable dates, counted from
	// 2000-01-01 the way the binary format does.
	const (
		minDayOffset = -2451545   // 4713-11-24 BC
		maxDayOffset = 2145031948 // 5874897-12-31
	)

	src := func(dayOffset int32) []byte {
		return binary.BigEndian.AppendUint32(nil, uint32(dayOffset))
	}

	for _, tt := range []struct {
		dayOffset int32
		want      time.Time
	}{
		{0, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{minDayOffset, time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)},
		{maxDayOffset, time.Date(5874897, 12, 31, 0, 0, 0, 0, time.UTC)},
	} {
		var d pgtype.Date
		plan := c.PlanScan(nil, pgtype.DateOID, pgtype.BinaryFormatCode, &d)

		require.NoErrorf(t, plan.Scan(src(tt.dayOffset), &d), "%d", tt.dayOffset)
		require.Truef(t, d.Valid, "%d", tt.dayOffset)
		require.Equalf(t, tt.want, d.Time, "%d", tt.dayOffset)
	}

	// The infinities are the int32 extremes and are handled before the range check, so
	// these stop short of them.
	for _, dayOffset := range []int32{minDayOffset - 1, maxDayOffset + 1, -2147483000, 2147483000} {
		var d pgtype.Date
		plan := c.PlanScan(nil, pgtype.DateOID, pgtype.BinaryFormatCode, &d)

		require.Errorf(t, plan.Scan(src(dayOffset), &d), "%d", dayOffset)
	}
}
