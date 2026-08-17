package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/internal/pgio"
)

const (
	pgTimestampFormat  = "2006-01-02 15:04:05.999999999"
	jsonISO8601        = "2006-01-02T15:04:05.999999999"
	maxTimestampYear   = 294276
	maxTimestampBCYear = 4714
)

type TimestampScanner interface {
	ScanTimestamp(v Timestamp) error
}

type TimestampValuer interface {
	TimestampValue() (Timestamp, error)
}

// Timestamp represents the PostgreSQL timestamp type.
type Timestamp struct {
	Time             time.Time // Time zone will be ignored when encoding to PostgreSQL.
	InfinityModifier InfinityModifier
	Valid            bool
}

// ScanTimestamp implements the [TimestampScanner] interface.
func (ts *Timestamp) ScanTimestamp(v Timestamp) error {
	*ts = v
	return nil
}

// TimestampValue implements the [TimestampValuer] interface.
func (ts Timestamp) TimestampValue() (Timestamp, error) {
	return ts, nil
}

// Scan implements the [database/sql.Scanner] interface.
func (ts *Timestamp) Scan(src any) error {
	if src == nil {
		*ts = Timestamp{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return (&scanPlanTextTimestampToTimestampScanner{}).Scan([]byte(src), ts)
	case time.Time:
		*ts = Timestamp{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the [database/sql/driver.Valuer] interface.
func (ts Timestamp) Value() (driver.Value, error) {
	if !ts.Valid {
		return nil, nil
	}

	if ts.InfinityModifier != Finite {
		return ts.InfinityModifier.String(), nil
	}
	return ts.Time, nil
}

// MarshalJSON implements the [encoding/json.Marshaler] interface.
func (ts Timestamp) MarshalJSON() ([]byte, error) {
	if !ts.Valid {
		return []byte("null"), nil
	}

	var s string

	switch ts.InfinityModifier {
	case Finite:
		s = ts.Time.Format(jsonISO8601)
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return json.Marshal(s)
}

// UnmarshalJSON implements the [encoding/json.Unmarshaler] interface.
func (ts *Timestamp) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*ts = Timestamp{}
		return nil
	}

	switch *s {
	case "infinity":
		*ts = Timestamp{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*ts = Timestamp{Valid: true, InfinityModifier: -Infinity}
	default:
		// Parse time with or without timezone
		tss := *s
		// PostgreSQL uses ISO 8601 without timezone for to_json function and casting from a string to timestamp
		tim, err := time.Parse(time.RFC3339Nano, tss)
		if err == nil {
			*ts = Timestamp{Time: tim, Valid: true}
			return nil
		}
		tim, err = time.ParseInLocation(jsonISO8601, tss, time.UTC)
		if err == nil {
			*ts = Timestamp{Time: tim, Valid: true}
			return nil
		}
		ts.Valid = false
		return fmt.Errorf("cannot unmarshal %s to timestamp with layout %s or %s (%w)",
			*s, time.RFC3339Nano, jsonISO8601, err)
	}
	return nil
}

type TimestampCodec struct {
	// ScanLocation is the location that the time is assumed to be in for scanning. This is different from
	// TimestamptzCodec.ScanLocation in that this setting does change the instant in time that the timestamp represents.
	ScanLocation *time.Location
}

func (*TimestampCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (*TimestampCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (*TimestampCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	if _, ok := value.(TimestampValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanTimestampCodecBinary{}
	case TextFormatCode:
		return encodePlanTimestampCodecText{}
	}

	return nil
}

type encodePlanTimestampCodecBinary struct{}

func (encodePlanTimestampCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimestampValuer).TimestampValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	var microsecSinceY2K int64
	switch ts.InfinityModifier {
	case Finite:
		t := discardTimeZone(ts.Time)
		microsecSinceUnixEpoch := t.Unix()*1_000_000 + int64(t.Nanosecond())/1000
		microsecSinceY2K = microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
	case Infinity:
		microsecSinceY2K = infinityMicrosecondOffset
	case NegativeInfinity:
		microsecSinceY2K = negativeInfinityMicrosecondOffset
	}

	buf = pgio.AppendInt64(buf, microsecSinceY2K)

	return buf, nil
}

type encodePlanTimestampCodecText struct{}

func (encodePlanTimestampCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimestampValuer).TimestampValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	var s string

	switch ts.InfinityModifier {
	case Finite:
		t := discardTimeZone(ts.Time)

		// Year 0000 is 1 BC
		bc := false
		if year := t.Year(); year <= 0 {
			year = -year + 1
			t = time.Date(year, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			bc = true
		}

		s = t.Truncate(time.Microsecond).Format(pgTimestampFormat)

		if bc {
			s += " BC"
		}
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	buf = append(buf, s...)

	return buf, nil
}

func discardTimeZone(t time.Time) time.Time {
	if t.Location() != time.UTC {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	}

	return t
}

func (c *TimestampCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {
	switch format {
	case BinaryFormatCode:
		if _, ok := target.(TimestampScanner); ok {
			return &scanPlanBinaryTimestampToTimestampScanner{location: c.ScanLocation}
		}
	case TextFormatCode:
		if _, ok := target.(TimestampScanner); ok {
			return &scanPlanTextTimestampToTimestampScanner{location: c.ScanLocation}
		}
	}

	return nil
}

type scanPlanBinaryTimestampToTimestampScanner struct{ location *time.Location }

func (plan *scanPlanBinaryTimestampToTimestampScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TimestampScanner)

	if src == nil {
		return scanner.ScanTimestamp(Timestamp{})
	}

	raw, err := pgio.Uint64Exact(src)
	if err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}

	var ts Timestamp
	microsecSinceY2K := int64(raw)

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		ts = Timestamp{Valid: true, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		ts = Timestamp{Valid: true, InfinityModifier: -Infinity}
	default:
		tim := time.Unix(
			microsecFromUnixEpochToY2K/1_000_000+microsecSinceY2K/1_000_000,
			(microsecFromUnixEpochToY2K%1_000_000*1_000)+(microsecSinceY2K%1_000_000*1000),
		).UTC()
		if plan.location != nil {
			tim = time.Date(tim.Year(), tim.Month(), tim.Day(), tim.Hour(), tim.Minute(), tim.Second(), tim.Nanosecond(), plan.location)
		}
		ts = Timestamp{Time: tim, Valid: true}
	}

	return scanner.ScanTimestamp(ts)
}

type scanPlanTextTimestampToTimestampScanner struct{ location *time.Location }

func (plan *scanPlanTextTimestampToTimestampScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TimestampScanner)

	if src == nil {
		return scanner.ScanTimestamp(Timestamp{})
	}

	var ts Timestamp
	sbuf := string(src)
	switch sbuf {
	case "infinity":
		ts = Timestamp{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		ts = Timestamp{Valid: true, InfinityModifier: -Infinity}
	default:
		bc := false
		if strings.HasSuffix(sbuf, " BC") {
			sbuf = sbuf[:len(sbuf)-3]
			bc = true
		}
		maxYear := int64(maxTimestampYear)
		if bc {
			maxYear = maxTimestampBCYear
		}
		tim, err := parseTimestampWithVariableYear(pgTimestampFormat, sbuf, false, maxYear, bc)
		if err != nil {
			return err
		}
		if timestampOutOfRange(tim) {
			return fmt.Errorf("timestamp out of range")
		}

		if plan.location != nil {
			tim = time.Date(tim.Year(), tim.Month(), tim.Day(), tim.Hour(), tim.Minute(), tim.Second(), tim.Nanosecond(), plan.location)
		}

		ts = Timestamp{Time: tim, Valid: true}
	}

	return scanner.ScanTimestamp(ts)
}

func parseTimestampWithVariableYear(layout, s string, preserveOffset bool, maxYear int64, bc bool) (time.Time, error) {
	yearEnd, err := timestampYearEnd(s)
	if err != nil {
		return time.Time{}, err
	}
	if yearEnd == 4 && !bc {
		if _, err := parseTimestampYear(s[:yearEnd], maxYear); err != nil {
			return time.Time{}, err
		}
		tim, err := time.Parse(layout, s)
		if err != nil {
			return time.Time{}, err
		}
		if preserveOffset {
			_, offset := tim.Zone()
			if offset <= -16*60*60 || offset >= 16*60*60 {
				return time.Time{}, fmt.Errorf("time zone displacement out of range")
			}
		}
		return tim, nil
	}

	normalized, year, err := normalizeTimestampYear(s, maxYear, bc)
	if err != nil {
		return time.Time{}, err
	}

	tim, err := time.Parse(layout, normalized)
	if err != nil {
		return time.Time{}, err
	}

	loc := tim.Location()
	if preserveOffset {
		_, offset := tim.Zone()
		if offset <= -16*60*60 || offset >= 16*60*60 {
			return time.Time{}, fmt.Errorf("time zone displacement out of range")
		}
		loc = time.FixedZone("", offset)
	}

	if bc {
		year = 1 - year
	}

	return time.Date(year, tim.Month(), tim.Day(), tim.Hour(), tim.Minute(), tim.Second(), tim.Nanosecond(), loc), nil
}

func timestampOutOfRange(t time.Time) bool {
	return t.Before(minTimestampTime().Add(-500*time.Nanosecond)) || !t.Before(maxTimestampTime().Add(500*time.Nanosecond))
}

func minTimestampTime() time.Time {
	return time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC)
}

func maxTimestampTime() time.Time {
	return time.Date(maxTimestampYear, 12, 31, 23, 59, 59, 999999000, time.UTC)
}

func normalizeTimestampYear(s string, maxYear int64, bc bool) (string, int, error) {
	yearEnd, err := timestampYearEnd(s)
	if err != nil {
		return "", 0, err
	}

	year64, err := parseTimestampYear(s[:yearEnd], maxYear)
	if err != nil {
		return "", 0, err
	}
	year := int(year64)

	normalizedYear := "2001"
	if isTimestampLeapYear(year, bc) {
		normalizedYear = "2000"
	}

	return normalizedYear + s[yearEnd:], year, nil
}

func timestampYearEnd(s string) (int, error) {
	yearEnd := -1
	for i := 4; i < len(s); i++ {
		if s[i] == '-' {
			yearEnd = i
			break
		}
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("invalid timestamp format")
		}
	}
	if yearEnd == -1 {
		return 0, fmt.Errorf("invalid timestamp format")
	}
	return yearEnd, nil
}

func parseTimestampYear(s string, maxYear int64) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("invalid timestamp format")
	}

	var n int64
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid timestamp format")
		}
		digit := int64(c - '0')
		if n > (maxYear-digit)/10 {
			return 0, fmt.Errorf("timestamp year out of range")
		}
		n = n*10 + digit
	}
	if n < 1 || n > maxYear {
		return 0, fmt.Errorf("timestamp year out of range")
	}

	return n, nil
}

func isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func isTimestampLeapYear(year int, bc bool) bool {
	if bc {
		year = 1 - year
	}
	return isLeapYear(year)
}

func (c *TimestampCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var ts Timestamp
	err := codecScan(c, m, oid, format, src, &ts)
	if err != nil {
		return nil, err
	}

	if ts.InfinityModifier != Finite {
		return ts.InfinityModifier.String(), nil
	}

	return ts.Time, nil
}

func (c *TimestampCodec) DecodeValue(m *Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var ts Timestamp
	err := codecScan(c, m, oid, format, src, &ts)
	if err != nil {
		return nil, err
	}

	if ts.InfinityModifier != Finite {
		return ts.InfinityModifier, nil
	}

	return ts.Time, nil
}
