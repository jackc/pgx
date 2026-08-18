package pgtype

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// PostgreSQL's ISO date/time output grammar, from EncodeDateTime, EncodeTimezone and
// AppendSeconds in src/backend/utils/adt/datetime.c (the USE_ISO_DATES branch):
//
//	value  = "infinity" / "-infinity" / stamp
//	stamp  = date [ SP time [ offset ] ] [ " BC" ]
//	date   = year "-" 2DIGIT "-" 2DIGIT
//	year   = 4*DIGIT     ; zero-padded to exactly 4, never wider than necessary
//	time   = 2DIGIT ":" 2DIGIT ":" 2DIGIT [ "." 1*6DIGIT ]
//	offset = ("+" / "-") 2DIGIT [ ":" 2DIGIT [ ":" 2DIGIT ] ]
//
// " BC" is always last, after any offset. Fractional seconds are omitted entirely when
// zero and never carry trailing zeros. This is only the shape the server emits under the
// default ISO DateStyle; the other styles are not supported.
//
// Writing the format is the other half of this and lives in internal/pgdatetime, which the
// simple protocol's query sanitizer shares.

const (
	// maxTZDisplacementHour is PostgreSQL's MAX_TZDISP_HOUR.
	maxTZDisplacementHour = 15

	secondsPerHour = 60 * 60

	// maxDateTimeYearDigits is the width of the largest year PostgreSQL can represent,
	// which is date's 5874897. Capping the digit count keeps the accumulated year inside
	// an int on 32 bit platforms as well as 64 bit ones.
	maxDateTimeYearDigits = 7
)

// PostgreSQL's valid ranges, from src/include/datatype/timestamp.h. They differ by type:
// date runs to 5874897 AD (JULIAN_MAXYEAR) while timestamp stops at 294276 AD
// (END_TIMESTAMP). Both share a lower bound of 4714-11-24 BC.
var (
	minDateTime  = time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)
	endDate      = time.Date(5874898, 1, 1, 0, 0, 0, 0, time.UTC)
	endTimestamp = time.Date(294277, 1, 1, 0, 0, 0, 0, time.UTC)
)

// textDateTime is a date/time value parsed from PostgreSQL's text format. It holds the
// fields as written on the wire rather than a time.Time so that each codec can apply its
// own range limits and time zone rules.
type textDateTime struct {
	// year uses astronomical numbering: 1 BC is 0, 2 BC is -1.
	year  int
	month int
	day   int

	hour int
	min  int
	sec  int
	// nsec is always a multiple of 1000. Fractional seconds are rounded to microseconds
	// the way the server rounds them.
	nsec int

	// offset is seconds east of UTC and is only meaningful when hasOffset is true.
	offset int

	hasTime   bool
	hasOffset bool

	infinity InfinityModifier
}

// in builds the parsed fields as a time.Time in loc. The fields are a wall clock reading,
// so this reinterprets them in loc rather than converting an instant into it.
func (dt textDateTime) in(loc *time.Location) time.Time {
	return time.Date(dt.year, time.Month(dt.month), dt.day, dt.hour, dt.min, dt.sec, dt.nsec, loc)
}

// toTime resolves the parsed fields to the instant they name, applying any time zone
// offset, and rejects anything outside the range of the type named by name. The upper
// limit differs by type -- date reaches 5874897 AD while timestamp stops at 294276 AD --
// so end is supplied by the caller and is exclusive. src is only used to build the error.
func (dt textDateTime) toTime(src []byte, name string, end time.Time) (time.Time, error) {
	t := dt.in(time.UTC).Add(-time.Duration(dt.offset) * time.Second)
	if t.Before(minDateTime) || !t.Before(end) {
		return time.Time{}, fmt.Errorf("%s %q is out of range", name, src)
	}

	return t, nil
}

// parseTextDateTime parses the grammar above. It validates the grammar and the calendar
// but applies no range limits, because PostgreSQL's limits differ by type: date reaches
// 5874897 AD while timestamp stops at 294276 AD.
func parseTextDateTime(src []byte) (textDateTime, error) {
	var dt textDateTime

	switch string(src) {
	case "infinity":
		dt.infinity = Infinity
		return dt, nil
	case "-infinity":
		dt.infinity = NegativeInfinity
		return dt, nil
	}

	s := src
	bc := false
	if len(s) > 3 && s[len(s)-3] == ' ' && s[len(s)-2] == 'B' && s[len(s)-1] == 'C' {
		s = s[:len(s)-3]
		bc = true
	}

	i := 0

	// Year. Variable width, so it runs to the first non-digit rather than a fixed offset.
	yearStart := i
	for i < len(s) && isDigit(s[i]) {
		if i-yearStart >= maxDateTimeYearDigits {
			return dt, badDateTime(src)
		}
		dt.year = dt.year*10 + int(s[i]-'0')
		i++
	}
	if i-yearStart < 4 || dt.year < 1 {
		return dt, badDateTime(src)
	}

	var ok bool
	if i, ok = expect(s, i, '-'); !ok {
		return dt, badDateTime(src)
	}
	if dt.month, i, ok = parse2Digits(s, i); !ok {
		return dt, badDateTime(src)
	}
	if i, ok = expect(s, i, '-'); !ok {
		return dt, badDateTime(src)
	}
	if dt.day, i, ok = parse2Digits(s, i); !ok {
		return dt, badDateTime(src)
	}

	if bc {
		dt.year = 1 - dt.year
	}

	if dt.month < 1 || dt.month > 12 || dt.day < 1 || dt.day > daysInMonth(dt.year, dt.month) {
		return dt, badDateTime(src)
	}

	// Time of day.
	var carrySecond bool
	if i < len(s) && s[i] == ' ' {
		i++
		dt.hasTime = true

		if dt.hour, i, ok = parse2Digits(s, i); !ok {
			return dt, badDateTime(src)
		}
		if i, ok = expect(s, i, ':'); !ok {
			return dt, badDateTime(src)
		}
		if dt.min, i, ok = parse2Digits(s, i); !ok {
			return dt, badDateTime(src)
		}
		if i, ok = expect(s, i, ':'); !ok {
			return dt, badDateTime(src)
		}
		if dt.sec, i, ok = parse2Digits(s, i); !ok {
			return dt, badDateTime(src)
		}
		if dt.hour > 23 || dt.min > 59 || dt.sec > 59 {
			return dt, badDateTime(src)
		}

		if i < len(s) && s[i] == '.' {
			i++
			fracStart := i
			for i < len(s) && isDigit(s[i]) {
				i++
			}
			if i == fracStart {
				return dt, badDateTime(src)
			}

			usec, err := roundFractionToMicroseconds(s[fracStart:i])
			if err != nil {
				return dt, badDateTime(src)
			}
			if usec == microsecondsPerSecond {
				usec = 0
				carrySecond = true
			}
			dt.nsec = usec * 1000
		}

		// Time zone displacement.
		if i < len(s) {
			switch s[i] {
			case 'Z':
				// Not something the server emits, but pgx's own text encoder writes it.
				i++
				dt.hasOffset = true
			case '+', '-':
				if dt.offset, i, ok = parseOffset(s, i); !ok {
					return dt, badDateTime(src)
				}
				dt.hasOffset = true
			}
		}
	}

	if i != len(s) {
		return dt, badDateTime(src)
	}

	if carrySecond {
		dt.addSecond()
	}

	return dt, nil
}

// addSecond advances by one second, carrying through to the year. It is only reached when
// rounding the fractional part rolls over, and it assumes the fields are already valid.
func (dt *textDateTime) addSecond() {
	dt.sec++
	if dt.sec < 60 {
		return
	}
	dt.sec = 0

	dt.min++
	if dt.min < 60 {
		return
	}
	dt.min = 0

	dt.hour++
	if dt.hour < 24 {
		return
	}
	dt.hour = 0

	dt.day++
	if dt.day <= daysInMonth(dt.year, dt.month) {
		return
	}
	dt.day = 1

	dt.month++
	if dt.month <= 12 {
		return
	}
	dt.month = 1

	// Astronomical numbering has a year 0, so this needs no adjustment for the AD/BC
	// boundary.
	dt.year++
}

// roundFractionToMicroseconds converts fractional second digits to microseconds. The
// result may be microsecondsPerSecond, which the caller must carry.
func roundFractionToMicroseconds(digits []byte) (int, error) {
	if len(digits) <= 6 {
		usec := 0
		for _, c := range digits {
			usec = usec*10 + int(c-'0')
		}
		for i := len(digits); i < 6; i++ {
			usec *= 10
		}
		return usec, nil
	}

	// PostgreSQL's ParseFractionalSecond reads the fraction with strtod and rounds with
	// rint, which is round-half-to-even on a binary double. Mirror that arithmetic
	// exactly rather than rounding the decimal digits, so that values the server rounds
	// down are not rounded up here: the server turns .0000005 into 0, while rounding the
	// digits half away from zero would produce 1 microsecond.
	//
	// The server never emits more than six digits, so this path only handles input from
	// other sources.
	f, err := strconv.ParseFloat("0."+string(digits), 64)
	if err != nil {
		return 0, err
	}

	return int(math.RoundToEven(f * 1e6)), nil
}

func parseOffset(s []byte, i int) (offset, next int, ok bool) {
	neg := s[i] == '-'
	i++

	var hour, min, sec int
	if hour, i, ok = parse2Digits(s, i); !ok {
		return 0, i, false
	}
	if i, ok = expect(s, i, ':'); ok {
		if min, i, ok = parse2Digits(s, i); !ok {
			return 0, i, false
		}
		if i, ok = expect(s, i, ':'); ok {
			if sec, i, ok = parse2Digits(s, i); !ok {
				return 0, i, false
			}
		}
	}

	if hour > maxTZDisplacementHour || min > 59 || sec > 59 {
		return 0, i, false
	}

	offset = hour*secondsPerHour + min*60 + sec
	if neg {
		offset = -offset
	}

	return offset, i, true
}

func parse2Digits(s []byte, i int) (v, next int, ok bool) {
	if i+1 >= len(s) || !isDigit(s[i]) || !isDigit(s[i+1]) {
		return 0, i, false
	}
	return int(s[i]-'0')*10 + int(s[i+1]-'0'), i + 2, true
}

func expect(s []byte, i int, c byte) (next int, ok bool) {
	if i >= len(s) || s[i] != c {
		return i, false
	}
	return i + 1, true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func badDateTime(src []byte) error {
	const maxLen = 64
	s := string(src)
	if len(s) > maxLen {
		s = s[:maxLen] + "..."
	}
	return fmt.Errorf("invalid PostgreSQL date/time value %q", s)
}

// isLeapYear reports whether year, in astronomical numbering, is a leap year in the
// proleptic Gregorian calendar. Go's % keeps the sign of the dividend, so the usual
// divisibility tests hold for years at or below zero.
func isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func daysInMonth(year, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	}
	return 0
}
