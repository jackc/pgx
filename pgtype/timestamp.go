package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgio"
)

const pgTimestampFormat = "2006-01-02 15:04:05.999999999"

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

func (ts *Timestamp) ScanTimestamp(v Timestamp) error {
	*ts = v
	return nil
}

func (ts Timestamp) TimestampValue() (Timestamp, error) {
	return ts, nil
}

// Scan implements the database/sql Scanner interface.
func (ts *Timestamp) Scan(src interface{}) error {
	if src == nil {
		*ts = Timestamp{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextTimestampToTimestampScanner{}.Scan(nil, 0, TextFormatCode, []byte(src), ts)
	case time.Time:
		*ts = Timestamp{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (ts Timestamp) Value() (driver.Value, error) {
	if !ts.Valid {
		return nil, nil
	}

	if ts.InfinityModifier != None {
		return ts.InfinityModifier.String(), nil
	}
	return ts.Time, nil
}

type TimestampCodec struct{}

func (TimestampCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (TimestampCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (TimestampCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
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

func (encodePlanTimestampCodecBinary) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimestampValuer).TimestampValue()
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	var microsecSinceY2K int64
	switch ts.InfinityModifier {
	case None:
		t := discardTimeZone(ts.Time)
		microsecSinceUnixEpoch := t.Unix()*1000000 + int64(t.Nanosecond())/1000
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

func (encodePlanTimestampCodecText) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	ts, err := value.(TimestampValuer).TimestampValue()
	if err != nil {
		return nil, err
	}

	var s string

	switch ts.InfinityModifier {
	case None:
		t := discardTimeZone(ts.Time)
		s = t.Truncate(time.Microsecond).Format(pgTimestampFormat)
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

func (TimestampCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {

	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case TimestampScanner:
			return scanPlanBinaryTimestampToTimestampScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case TimestampScanner:
			return scanPlanTextTimestampToTimestampScanner{}
		}
	}

	return nil
}

type scanPlanBinaryTimestampToTimestampScanner struct{}

func (scanPlanBinaryTimestampToTimestampScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	scanner := (dst).(TimestampScanner)

	if src == nil {
		return scanner.ScanTimestamp(Timestamp{})
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for timestamp: %v", len(src))
	}

	var ts Timestamp
	microsecSinceY2K := int64(binary.BigEndian.Uint64(src))

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		ts = Timestamp{Valid: true, InfinityModifier: Infinity}
	case negativeInfinityMicrosecondOffset:
		ts = Timestamp{Valid: true, InfinityModifier: -Infinity}
	default:
		tim := time.Unix(
			microsecFromUnixEpochToY2K/1000000+microsecSinceY2K/1000000,
			(microsecFromUnixEpochToY2K%1000000*1000)+(microsecSinceY2K%1000000*1000),
		).UTC()
		ts = Timestamp{Time: tim, Valid: true}
	}

	return scanner.ScanTimestamp(ts)
}

type scanPlanTextTimestampToTimestampScanner struct{}

func (scanPlanTextTimestampToTimestampScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
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
		tim, err := time.Parse(pgTimestampFormat, sbuf)
		if err != nil {
			return err
		}

		ts = Timestamp{Time: tim, Valid: true}
	}

	return scanner.ScanTimestamp(ts)
}

func (c TimestampCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var ts Timestamp
	err := codecScan(c, ci, oid, format, src, &ts)
	if err != nil {
		return nil, err
	}

	if ts.InfinityModifier != None {
		return ts.InfinityModifier.String(), nil
	}

	return ts.Time, nil
}

func (c TimestampCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var ts Timestamp
	err := codecScan(c, ci, oid, format, src, &ts)
	if err != nil {
		return nil, err
	}

	if ts.InfinityModifier != None {
		return ts.InfinityModifier, nil
	}

	return ts.Time, nil
}
