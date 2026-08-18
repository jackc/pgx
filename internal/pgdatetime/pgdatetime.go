// Package pgdatetime writes PostgreSQL's ISO date/time text format.
//
// The format is the one EncodeDateTime and AppendSeconds produce in
// src/backend/utils/adt/datetime.c under the default ISO DateStyle:
//
//	stamp = date [ SP time [ zone ] ] [ " BC" ]
//	date  = year "-" 2DIGIT "-" 2DIGIT
//	time  = 2DIGIT ":" 2DIGIT ":" 2DIGIT [ "." 1*6DIGIT ]
//
// The year is zero-padded to four digits and then grows as wide as it needs, the
// fractional part is omitted when it is zero and never carries trailing zeros, and " BC"
// comes last, after any time zone. Go's layout language can express none of those, which
// is why the format is written out here rather than left to time.Format.
//
// This is the only implementation of the format in pgx. pgtype's date, timestamp and
// timestamptz text encoders and the simple protocol's query sanitizer all go through it,
// so they cannot drift apart.
package pgdatetime

import (
	"strconv"
	"time"
)

// AppendDate appends t's date. The fields are read from t in its own location.
func AppendDate(buf []byte, t time.Time) []byte {
	year, bc := splitBCYear(t.Year())
	buf = appendDate(buf, year, int(t.Month()), t.Day())
	return appendEra(buf, bc)
}

// AppendTimestamp appends t's date and time of day followed by zone, which goes before
// the era suffix because PostgreSQL writes " BC" last. zone is "Z" for a value already
// converted to UTC and "" for a type that carries no time zone at all.
//
// The fields are read from t in its own location, so a caller that wants the UTC instant
// must convert first. Sub-microsecond precision is discarded, matching the resolution of
// the wire format.
func AppendTimestamp(buf []byte, t time.Time, zone string) []byte {
	year, bc := splitBCYear(t.Year())
	buf = appendDate(buf, year, int(t.Month()), t.Day())
	buf = append(buf, ' ')
	buf = appendTime(buf, t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
	buf = append(buf, zone...)
	return appendEra(buf, bc)
}

// splitBCYear converts an astronomical year, in which 1 BC is 0 and 2 BC is -1, to the
// year and era PostgreSQL writes.
func splitBCYear(year int) (displayYear int, bc bool) {
	if year <= 0 {
		return 1 - year, true
	}
	return year, false
}

func appendEra(buf []byte, bc bool) []byte {
	if bc {
		buf = append(buf, " BC"...)
	}
	return buf
}

// appendDate appends year-month-day. year is a displayed year from splitBCYear rather
// than an astronomical one.
func appendDate(buf []byte, year, month, day int) []byte {
	buf = appendYear(buf, year)
	buf = append(buf, '-')
	buf = append2Digits(buf, month)
	buf = append(buf, '-')
	return append2Digits(buf, day)
}

func appendTime(buf []byte, hour, min, sec, nsec int) []byte {
	buf = append2Digits(buf, hour)
	buf = append(buf, ':')
	buf = append2Digits(buf, min)
	buf = append(buf, ':')
	buf = append2Digits(buf, sec)

	usec := nsec / 1000
	if usec == 0 {
		return buf
	}

	var frac [6]byte
	for i := 5; i >= 0; i-- {
		frac[i] = byte('0' + usec%10)
		usec /= 10
	}

	// The server trims trailing zeros, so ".5" rather than ".500000".
	end := len(frac)
	for end > 1 && frac[end-1] == '0' {
		end--
	}

	buf = append(buf, '.')
	return append(buf, frac[:end]...)
}

// append2Digits appends v as exactly two digits. Every field but the year is fixed width
// and comes from a time.Time accessor, so v is always in [0, 99] and the general routine
// below is not needed.
func append2Digits(buf []byte, v int) []byte {
	return append(buf, byte('0'+v/10), byte('0'+v%10))
}

// appendYear appends v zero-padded to four digits, or wider when the year needs it. v is
// a displayed year from splitBCYear and so is always positive.
func appendYear(buf []byte, v int) []byte {
	var tmp [20]byte
	s := strconv.AppendInt(tmp[:0], int64(v), 10)
	for i := len(s); i < 4; i++ {
		buf = append(buf, '0')
	}
	return append(buf, s...)
}
