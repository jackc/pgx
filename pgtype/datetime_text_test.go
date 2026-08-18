package pgtype

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/pgdatetime"
)

func TestParseTextDateTime(t *testing.T) {
	for _, tt := range []struct {
		src  string
		want textDateTime
	}{
		// Dates.
		{"2024-01-02", textDateTime{year: 2024, month: 1, day: 2}},
		{"0001-01-01", textDateTime{year: 1, month: 1, day: 1}},
		{"10000-01-02", textDateTime{year: 10000, month: 1, day: 2}},
		{"5874897-12-31", textDateTime{year: 5874897, month: 12, day: 31}},

		// BC uses astronomical numbering, so 1 BC is year 0.
		{"0001-01-01 BC", textDateTime{year: 0, month: 1, day: 1}},
		{"4713-02-29 BC", textDateTime{year: -4712, month: 2, day: 29}},
		{"4714-11-24 BC", textDateTime{year: -4713, month: 11, day: 24}},

		// Leap days.
		{"2024-02-29", textDateTime{year: 2024, month: 2, day: 29}},
		{"2000-02-29", textDateTime{year: 2000, month: 2, day: 29}},
		{"10000-02-29", textDateTime{year: 10000, month: 2, day: 29}},

		// Times.
		{
			"2024-01-02 03:04:05",
			textDateTime{year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5, hasTime: true},
		},
		{
			"2024-01-02 03:04:05.123456",
			textDateTime{year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5, nsec: 123456000, hasTime: true},
		},
		{
			"2024-01-02 03:04:05.5",
			textDateTime{year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5, nsec: 500000000, hasTime: true},
		},
		{
			"2024-01-02 23:59:59.999999",
			textDateTime{year: 2024, month: 1, day: 2, hour: 23, min: 59, sec: 59, nsec: 999999000, hasTime: true},
		},

		// Offsets widen only as far as they need to.
		{
			"2024-01-02 03:04:05+00",
			textDateTime{year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5, hasTime: true, hasOffset: true},
		},
		{
			"2024-01-02 03:04:05-05",
			textDateTime{
				year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5,
				offset: -5 * 60 * 60, hasTime: true, hasOffset: true,
			},
		},
		{
			"2024-01-02 03:04:05+05:30",
			textDateTime{
				year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5,
				offset: 5*60*60 + 30*60, hasTime: true, hasOffset: true,
			},
		},
		{
			"1883-11-18 12:03:57-04:56:02",
			textDateTime{
				year: 1883, month: 11, day: 18, hour: 12, min: 3, sec: 57,
				offset: -(4*60*60 + 56*60 + 2), hasTime: true, hasOffset: true,
			},
		},
		{
			"2024-01-02 03:04:05+15:59:59",
			textDateTime{
				year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5,
				offset: 15*60*60 + 59*60 + 59, hasTime: true, hasOffset: true,
			},
		},
		{
			// Not something the server emits, but pgx's own text encoder writes it.
			"2024-01-02 03:04:05Z",
			textDateTime{year: 2024, month: 1, day: 2, hour: 3, min: 4, sec: 5, hasTime: true, hasOffset: true},
		},

		// Offset and BC together; " BC" comes last.
		{
			"4714-11-24 00:00:00+00 BC",
			textDateTime{year: -4713, month: 11, day: 24, hasTime: true, hasOffset: true},
		},

		{"infinity", textDateTime{infinity: Infinity}},
		{"-infinity", textDateTime{infinity: NegativeInfinity}},
	} {
		got, err := parseTextDateTime([]byte(tt.src))
		if err != nil {
			t.Errorf("parseTextDateTime(%q): unexpected error: %v", tt.src, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseTextDateTime(%q):\n got %+v\nwant %+v", tt.src, got, tt.want)
		}
	}
}

// TestParseTextDateTimeRoundsFraction pins the behavior of PostgreSQL's
// ParseFractionalSecond, which rounds to microseconds with rint - round half to even - and
// carries the result into the rest of the value. Every expectation here was taken from a
// live PostgreSQL 18 server.
func TestParseTextDateTimeRoundsFraction(t *testing.T) {
	for _, tt := range []struct {
		src  string
		want string
	}{
		{"2024-01-01 00:00:00.9999994", "2024-01-01 00:00:00.999999"},
		{"2024-01-01 00:00:00.9999995", "2024-01-01 00:00:01"},
		{"2024-01-01 00:00:00.9999996", "2024-01-01 00:00:01"},

		// Ties round to even, so this is 0 rather than 1 microsecond. Rounding the decimal
		// digits half away from zero would disagree with the server here.
		{"2024-01-01 00:00:00.0000005", "2024-01-01 00:00:00"},
		{"2024-01-01 00:00:00.0000015", "2024-01-01 00:00:00.000002"},

		{"2024-01-01 00:00:00.123456789012345", "2024-01-01 00:00:00.123457"},

		// The carry propagates as far as it needs to.
		{"2024-01-01 23:59:59.9999999", "2024-01-02 00:00:00"},
		{"2024-01-31 23:59:59.9999999", "2024-02-01 00:00:00"},
		{"2024-12-31 23:59:59.9999999", "2025-01-01 00:00:00"},
		{"2024-02-28 23:59:59.9999999", "2024-02-29 00:00:00"},
		{"2023-02-28 23:59:59.9999999", "2023-03-01 00:00:00"},

		// Astronomical year numbering has a year 0, so a carry across it is unremarkable.
		{"0001-12-31 23:59:59.9999999 BC", "0001-01-01 00:00:00"},
	} {
		dt, err := parseTextDateTime([]byte(tt.src))
		if err != nil {
			t.Errorf("parseTextDateTime(%q): unexpected error: %v", tt.src, err)
			continue
		}

		buf := pgdatetime.AppendTimestamp(nil, dt.in(time.UTC), "")

		if string(buf) != tt.want {
			t.Errorf("parseTextDateTime(%q) = %q, want %q", tt.src, buf, tt.want)
		}
	}
}

func TestParseTextDateTimeErrors(t *testing.T) {
	for _, src := range []string{
		"",
		"eeeee",
		"2024",
		"2024-01",
		"999-01-01",           // year needs at least four digits
		"0000-01-01",          // there is no year zero in PostgreSQL's numbering
		"2024-1-1",            // month and day are always two digits
		"2024-13-01",          // month out of range
		"2024-00-01",          // month out of range
		"2024-02-30",          // day past the end of the month
		"2024-02-00",          // day out of range
		"2023-02-29",          // not a leap year
		"4712-02-29 BC",       // astronomical -4711, not a leap year
		"10001-02-29",         // not a leap year
		"1900-02-29",          // century that is not a leap year
		"2024-01-01T00:00:00", // ISO 8601 "T" separator is not PostgreSQL's ISO output
		"2024-01-01 24:00:00", // PostgreSQL never emits hour 24
		"2024-01-01 00:60:00",
		"2024-01-01 00:00:60",
		"2024-01-01 00:00:00.",   // fraction with no digits
		"2024-01-01 00:00:00+16", // beyond MAX_TZDISP_HOUR
		"2024-01-01 00:00:00-16", // beyond MAX_TZDISP_HOUR
		"2024-01-01 00:00:00+15:60",
		"2024-01-01 00:00:00+15:00:60",
		"2024-01-01 00:00:00+0", // offset hour is always two digits
		"2024-01-01 00:00:00 extra",
		"2024-01-01 00:00:00+00 AD", // PostgreSQL only ever writes " BC"
		"2024-01-01 BC extra",
		"99999999999999999999-01-01", // year long enough to overflow
		"infinityy",
		"-infinityy",
	} {
		if _, err := parseTextDateTime([]byte(src)); err == nil {
			t.Errorf("parseTextDateTime(%q): expected error, got none", src)
		}
	}
}

func TestIsLeapYear(t *testing.T) {
	for _, tt := range []struct {
		year int
		want bool
	}{
		{2024, true},
		{2023, false},
		{2000, true},
		{1900, false},

		// Astronomical numbering, where the divisibility rules have to keep working at and
		// below zero. Year 0 is 1 BC, year -4712 is 4713 BC.
		{0, true},
		{-1, false},
		{-4, true},
		{-4712, true},
		{-4711, false},
		{-100, false},
		{-400, true},
	} {
		if got := isLeapYear(tt.year); got != tt.want {
			t.Errorf("isLeapYear(%d) = %v, want %v", tt.year, got, tt.want)
		}
	}
}

// TestIsLeapYearMatchesTime cross-checks the leap year rule against the standard library
// across the whole range PostgreSQL can represent, including BC years.
func TestIsLeapYearMatchesTime(t *testing.T) {
	check := func(year int) {
		// February has 29 days exactly when March 1 minus one day is February 29.
		want := time.Date(year, 3, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1).Day() == 29
		if got := isLeapYear(year); got != want {
			t.Errorf("isLeapYear(%d) = %v, want %v", year, got, want)
		}
	}

	for year := -4713; year <= 2500; year++ {
		check(year)
	}
	for _, year := range []int{10000, 100000, 294276, 5874897, 1000000, 2000000} {
		check(year)
	}
}

// legacyTimestampLayout reports the Go layout that the previous, layout-based
// implementation would have used for src, and whether src is inside the domain that
// implementation could handle at all: a four digit AD year and no more fractional digits
// than time.Parse keeps. Year 0000 is excluded because time.Parse accepts it while
// PostgreSQL has no year zero.
func legacyTimestampLayout(src string) (string, bool) {
	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$`).MatchString(src) {
		return "", false
	}
	if strings.HasPrefix(src, "0000") {
		return "", false
	}
	return "2006-01-02 15:04:05.999999999", true
}

// FuzzParseTextDateTime checks two things. Against time.Parse it checks that the new
// parser agrees with the implementation it replaces everywhere both are defined, which is
// what makes it safe to swap in for ordinary four digit years. Independently it checks
// that anything the parser accepts survives a trip back out through the encoder, which
// covers the values time.Parse never could.
func FuzzParseTextDateTime(f *testing.F) {
	for _, seed := range []string{
		"2024-01-02 03:04:05",
		"2024-01-02 03:04:05.123456",
		"2024-01-02 03:04:05.5",
		"2024-02-29 00:00:00",
		"2023-02-29 00:00:00",
		"0001-01-01 00:00:00",
		"9999-12-31 23:59:59.999999",
		"2024-13-01 00:00:00",
		"2024-01-01 24:00:00",
		"2024-01-01",
		"10000-01-02 03:04:05.123456",
		"294276-12-31 23:59:59.999999",
		"4713-02-29 00:00:00 BC",
		"4714-11-24 00:00:00 BC",
		"2024-01-02 03:04:05+05:30",
		"1883-11-18 12:03:57-04:56:02",
		"2024-01-01 00:00:00.9999995",
		"2024-01-01 00:00:00.0000005",
		"infinity",
		"-infinity",
		"eeeee",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, src string) {
		dt, err := parseTextDateTime([]byte(src))

		if layout, ok := legacyTimestampLayout(src); ok {
			want, wantErr := time.Parse(layout, src)
			switch {
			case err != nil && wantErr == nil:
				t.Fatalf("parseTextDateTime(%q) failed with %v, but time.Parse accepted it", src, err)
			case err == nil && wantErr != nil:
				t.Fatalf("parseTextDateTime(%q) succeeded, but time.Parse failed with %v", src, wantErr)
			case err == nil:
				got := time.Date(dt.year, time.Month(dt.month), dt.day, dt.hour, dt.min, dt.sec, dt.nsec, time.UTC)
				if !got.Equal(want) {
					t.Fatalf("parseTextDateTime(%q) = %v, time.Parse = %v", src, got, want)
				}
			}
		}

		if err != nil || dt.infinity != Finite {
			return
		}

		if dt.nsec%1000 != 0 {
			t.Fatalf("parseTextDateTime(%q) produced sub-microsecond nsec %d", src, dt.nsec)
		}
		if dt.month < 1 || dt.month > 12 || dt.day < 1 || dt.day > daysInMonth(dt.year, dt.month) {
			t.Fatalf("parseTextDateTime(%q) produced an impossible date: %+v", src, dt)
		}

		// The offset is deliberately not preserved through the encoders, so only values
		// without one can be compared field for field.
		if dt.hasOffset {
			return
		}

		tim := dt.in(time.UTC)
		var buf []byte
		if dt.hasTime {
			buf = pgdatetime.AppendTimestamp(nil, tim, "")
		} else {
			buf = pgdatetime.AppendDate(nil, tim)
		}

		again, err := parseTextDateTime(buf)
		if err != nil {
			t.Fatalf("parseTextDateTime(%q) re-encoded to %q, which failed to parse: %v", src, buf, err)
		}
		if again != dt {
			t.Fatalf("parseTextDateTime(%q) did not survive a round trip through %q:\n got %+v\nwant %+v",
				src, buf, again, dt)
		}
	})
}

// TestParseTextDateTimeYearWidth pins the year width limit. The widest year PostgreSQL can
// represent is date's 5874897, and the cap keeps the accumulated year inside an int on 32
// bit platforms.
func TestParseTextDateTimeYearWidth(t *testing.T) {
	if _, err := parseTextDateTime([]byte("5874897-12-31")); err != nil {
		t.Errorf("widest representable year should parse: %v", err)
	}
	for _, src := range []string{
		"12345678-01-01",
		"0000001000-01-01",
		"99999999999999999999-01-01",
	} {
		if _, err := parseTextDateTime([]byte(src)); err == nil {
			t.Errorf("parseTextDateTime(%q): expected error, got none", src)
		}
	}
}
