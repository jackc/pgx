package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/internal/pgdatetime"
	"github.com/jackc/pgx/v5/internal/pgio"
)

const (
	microsecFromUnixEpochToY2K = 946_684_800 * 1_000_000
)

const (
	negativeInfinityMicrosecondOffset = -9223372036854775808
	infinityMicrosecondOffset         = 9223372036854775807
)

type TimestamptzScanner interface {
	ScanTimestamptz(v Timestamptz) error
}

type TimestamptzValuer interface {
	TimestamptzValue() (Timestamptz, error)
}

// Timestamptz represents the PostgreSQL timestamptz type.
type Timestamptz struct {
	Time             time.Time
	InfinityModifier InfinityModifier
	Valid            bool
}

// ScanTimestamptz implements the [TimestamptzScanner] interface.
func (tstz *Timestamptz) ScanTimestamptz(v Timestamptz) error {
	*tstz = v
	return nil
}

// TimestamptzValue implements the [TimestamptzValuer] interface.
func (tstz Timestamptz) TimestamptzValue() (Timestamptz, error) {
	return tstz, nil
}

// Scan implements the [database/sql.Scanner] interface.
func (tstz *Timestamptz) Scan(src any) error {
	if src == nil {
		*tstz = Timestamptz{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return (&scanPlanTextTimestamptzToTimestamptzScanner{}).Scan([]byte(src), tstz)
	case time.Time:
		*tstz = Timestamptz{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the [database/sql/driver.Valuer] interface.
func (tstz Timestamptz) Value() (driver.Value, error) {
	if !tstz.Valid {
		return nil, nil
	}

	if tstz.InfinityModifier != Finite {
		return tstz.InfinityModifier.String(), nil
	}
	return tstz.Time, nil
}

// MarshalJSON implements the [encoding/json.Marshaler] interface.
func (tstz Timestamptz) MarshalJSON() ([]byte, error) {
	if !tstz.Valid {
		return []byte("null"), nil
	}

	var s string

	switch tstz.InfinityModifier {
	case Finite:
		s = tstz.Time.Format(time.RFC3339Nano)
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return json.Marshal(s)
}

// UnmarshalJSON implements the [encoding/json.Unmarshaler] interface.
func (tstz *Timestamptz) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*tstz = Timestamptz{}
		return nil
	}

	switch *s {
	case "infinity":
		*tstz = Timestamptz{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*tstz = Timestamptz{Valid: true, InfinityModifier: -Infinity}
	default:
		// PostgreSQL uses ISO 8601 for to_json function and casting from a string to timestamptz
		tim, err := time.Parse(time.RFC3339Nano, *s)
		if err != nil {
			return err
		}

		*tstz = Timestamptz{Time: tim, Valid: true}
	}

	return nil
}

type TimestamptzCodec struct {
	// ScanLocation is the location to return scanned timestamptz values in. This does not change the instant in time that
	// the timestamptz represents.
	ScanLocation *time.Location
}

func (*TimestamptzCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (*TimestamptzCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (*TimestamptzCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	if _, ok := value.(TimestamptzValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanTimestamptzCodecBinary{}
	case TextFormatCode:
		return encodePlanTimestamptzCodecText{}
	}

	return nil
}

type encodePlanTimestamptzCodecBinary struct{}

func (encodePlanTimestamptzCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimestamptzValuer).TimestamptzValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	var microsecSinceY2K int64
	switch ts.InfinityModifier {
	case Finite:
		microsecSinceUnixEpoch := ts.Time.Unix()*1000000 + int64(ts.Time.Nanosecond())/1000
		microsecSinceY2K = microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
	case Infinity:
		microsecSinceY2K = infinityMicrosecondOffset
	case NegativeInfinity:
		microsecSinceY2K = negativeInfinityMicrosecondOffset
	}

	buf = pgio.AppendInt64(buf, microsecSinceY2K)

	return buf, nil
}

type encodePlanTimestamptzCodecText struct{}

func (encodePlanTimestamptzCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimestamptzValuer).TimestamptzValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	switch ts.InfinityModifier {
	case Finite:
		buf = pgdatetime.AppendTimestamp(buf, ts.Time.UTC(), "Z")
	case Infinity:
		buf = append(buf, "infinity"...)
	case NegativeInfinity:
		buf = append(buf, "-infinity"...)
	}

	return buf, nil
}

func (c *TimestamptzCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {
	switch format {
	case BinaryFormatCode:
		if _, ok := target.(TimestamptzScanner); ok {
			return &scanPlanBinaryTimestamptzToTimestamptzScanner{location: c.ScanLocation}
		}
	case TextFormatCode:
		if _, ok := target.(TimestamptzScanner); ok {
			return &scanPlanTextTimestamptzToTimestamptzScanner{location: c.ScanLocation}
		}
	}

	return nil
}

type scanPlanBinaryTimestamptzToTimestamptzScanner struct{ location *time.Location }

func (plan *scanPlanBinaryTimestamptzToTimestamptzScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TimestamptzScanner)

	if src == nil {
		return scanner.ScanTimestamptz(Timestamptz{})
	}

	raw, err := pgio.Uint64Exact(src)
	if err != nil {
		return fmt.Errorf("timestamptz: %w", err)
	}

	var tstz Timestamptz
	microsecSinceY2K := int64(raw)

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		tstz = Timestamptz{Valid: true, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		tstz = Timestamptz{Valid: true, InfinityModifier: -Infinity}
	default:
		tim := time.Unix(
			microsecFromUnixEpochToY2K/1_000_000+microsecSinceY2K/1_000_000,
			(microsecFromUnixEpochToY2K%1_000_000*1_000)+(microsecSinceY2K%1_000_000*1_000),
		)
		if tim.Before(minDateTime) || !tim.Before(endTimestamp) {
			return fmt.Errorf("timestamptz %d microseconds from 2000-01-01 is out of range", microsecSinceY2K)
		}
		if plan.location != nil {
			tim = tim.In(plan.location)
		}
		tstz = Timestamptz{Time: tim, Valid: true}
	}

	return scanner.ScanTimestamptz(tstz)
}

type scanPlanTextTimestamptzToTimestamptzScanner struct{ location *time.Location }

func (plan *scanPlanTextTimestamptzToTimestamptzScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(TimestamptzScanner)

	if src == nil {
		return scanner.ScanTimestamptz(Timestamptz{})
	}

	dt, err := parseTextDateTime(src)
	if err != nil {
		return err
	}

	var tstz Timestamptz
	if dt.infinity != Finite {
		tstz = Timestamptz{Valid: true, InfinityModifier: dt.infinity}
	} else {
		if !dt.hasTime || !dt.hasOffset {
			return badDateTime(src)
		}

		tim, err := dt.toTime(src, "timestamptz", endTimestamp)
		if err != nil {
			return err
		}

		// The binary path builds its value with time.Unix, which returns time.Local. Match
		// it, so that the same value scanned in either format produces the same time.Time.
		loc := time.Local
		if plan.location != nil {
			loc = plan.location
		}

		tstz = Timestamptz{Time: tim.In(loc), Valid: true}
	}

	return scanner.ScanTimestamptz(tstz)
}

func (c *TimestamptzCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var tstz Timestamptz
	err := codecScan(c, m, oid, format, src, &tstz)
	if err != nil {
		return nil, err
	}

	if tstz.InfinityModifier != Finite {
		return tstz.InfinityModifier.String(), nil
	}

	return tstz.Time, nil
}

func (c *TimestamptzCodec) DecodeValue(m *Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var tstz Timestamptz
	err := codecScan(c, m, oid, format, src, &tstz)
	if err != nil {
		return nil, err
	}

	if tstz.InfinityModifier != Finite {
		return tstz.InfinityModifier, nil
	}

	return tstz.Time, nil
}
