package pgdatetime_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/pgdatetime"
)

func date(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func TestAppendDate(t *testing.T) {
	for _, tt := range []struct {
		t    time.Time
		want string
	}{
		{date(2024, 1, 2), "2024-01-02"},
		{date(1, 1, 1), "0001-01-01"},
		{date(999, 12, 31), "0999-12-31"},
		{date(10000, 1, 2), "10000-01-02"},
		{date(294276, 12, 31), "294276-12-31"},
		{date(5874897, 12, 31), "5874897-12-31"},

		// Astronomical year 0 is 1 BC, and the era suffix comes last.
		{date(0, 1, 1), "0001-01-01 BC"},
		{date(-1, 12, 31), "0002-12-31 BC"},

		// February 29 of a BC leap year must survive as written. Astronomical -4712 is
		// divisible by 4, so it is a leap year even though 4713 is not.
		{date(-4712, 2, 29), "4713-02-29 BC"},
		{date(-4713, 11, 24), "4714-11-24 BC"},
	} {
		if got := string(pgdatetime.AppendDate(nil, tt.t)); got != tt.want {
			t.Errorf("AppendDate(%v) = %q, want %q", tt.t, got, tt.want)
		}
	}
}

func TestAppendTimestamp(t *testing.T) {
	stamp := func(hour, min, sec, nsec int) time.Time {
		return time.Date(2024, 1, 2, hour, min, sec, nsec, time.UTC)
	}

	for _, tt := range []struct {
		t    time.Time
		zone string
		want string
	}{
		{stamp(0, 0, 0, 0), "", "2024-01-02 00:00:00"},
		{stamp(1, 2, 3, 0), "", "2024-01-02 01:02:03"},
		{stamp(23, 59, 59, 999999000), "", "2024-01-02 23:59:59.999999"},

		// Trailing zeros are trimmed, and the fraction is omitted entirely when zero.
		{stamp(1, 2, 3, 500000000), "", "2024-01-02 01:02:03.5"},
		{stamp(1, 2, 3, 120000000), "", "2024-01-02 01:02:03.12"},
		{stamp(1, 2, 3, 1000), "", "2024-01-02 01:02:03.000001"},
		{stamp(1, 2, 3, 123456000), "", "2024-01-02 01:02:03.123456"},

		// Sub-microsecond precision is discarded rather than rounded, matching the
		// microsecond resolution of the wire format.
		{stamp(1, 2, 3, 500), "", "2024-01-02 01:02:03"},
		{stamp(1, 2, 3, 1500), "", "2024-01-02 01:02:03.000001"},

		// The zone goes after the time but before the era suffix.
		{stamp(1, 2, 3, 0), "Z", "2024-01-02 01:02:03Z"},
		{time.Date(0, 1, 1, 1, 2, 3, 0, time.UTC), "Z", "0001-01-01 01:02:03Z BC"},
		{time.Date(-4712, 2, 29, 1, 2, 3, 0, time.UTC), "", "4713-02-29 01:02:03 BC"},

		// Fields are read in t's own location, so a caller that wants UTC converts first.
		{stamp(1, 2, 3, 0).In(time.FixedZone("", 5*60*60)), "", "2024-01-02 06:02:03"},
	} {
		if got := string(pgdatetime.AppendTimestamp(nil, tt.t, tt.zone)); got != tt.want {
			t.Errorf("AppendTimestamp(%v, %q) = %q, want %q", tt.t, tt.zone, got, tt.want)
		}
	}
}

// TestAppendMatchesTimeFormat checks the encoders against time.Format over the range Go's
// layout language can express -- four digit AD years -- which is the range the codecs used
// to rely on it for.
func TestAppendMatchesTimeFormat(t *testing.T) {
	tim := time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)

	for tim.Before(end) {
		if got, want := string(pgdatetime.AppendDate(nil, tim)), tim.Format("2006-01-02"); got != want {
			t.Fatalf("AppendDate(%v) = %q, want %q", tim, got, want)
		}

		want := tim.Format("2006-01-02 15:04:05.999999")
		if got := string(pgdatetime.AppendTimestamp(nil, tim, "")); got != want {
			t.Fatalf("AppendTimestamp(%v) = %q, want %q", tim, got, want)
		}

		// A prime-ish step so the walk lands on every field rather than a fixed phase.
		tim = tim.Add(26*time.Hour + 37*time.Minute + 11*time.Second + 999983*time.Microsecond)
	}
}

// TestAppendBufferIsReused checks that the appenders write into the caller's buffer rather
// than allocating a fresh one, which is what makes them usable from the encode plans.
func TestAppendBufferIsReused(t *testing.T) {
	buf := make([]byte, 0, 64)
	buf = append(buf, "prefix "...)
	buf = pgdatetime.AppendTimestamp(buf, date(2024, 1, 2), "Z")

	if got, want := string(buf), "prefix 2024-01-02 00:00:00Z"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if cap(buf) != 64 {
		t.Errorf("buffer was reallocated: cap %d, want 64", cap(buf))
	}
}
