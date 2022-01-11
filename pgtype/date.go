package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgio"
)

type DateScanner interface {
	ScanDate(v Date) error
}

type DateValuer interface {
	DateValue() (Date, error)
}

type Date struct {
	Time             time.Time
	InfinityModifier InfinityModifier
	Valid            bool
}

func (d *Date) ScanDate(v Date) error {
	*d = v
	return nil
}

func (d Date) DateValue() (Date, error) {
	return d, nil
}

const (
	negativeInfinityDayOffset = -2147483648
	infinityDayOffset         = 2147483647
)

// Scan implements the database/sql Scanner interface.
func (dst *Date) Scan(src interface{}) error {
	if src == nil {
		*dst = Date{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextAnyToDateScanner{}.Scan(nil, 0, TextFormatCode, []byte(src), dst)
	case time.Time:
		*dst = Date{Time: src, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Date) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	if src.InfinityModifier != None {
		return src.InfinityModifier.String(), nil
	}
	return src.Time, nil
}

func (src Date) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	var s string

	switch src.InfinityModifier {
	case None:
		s = src.Time.Format("2006-01-02")
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return json.Marshal(s)
}

func (dst *Date) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*dst = Date{}
		return nil
	}

	switch *s {
	case "infinity":
		*dst = Date{Valid: true, InfinityModifier: Infinity}
	case "-infinity":
		*dst = Date{Valid: true, InfinityModifier: -Infinity}
	default:
		t, err := time.ParseInLocation("2006-01-02", *s, time.UTC)
		if err != nil {
			return err
		}

		*dst = Date{Time: t, Valid: true}
	}

	return nil
}

type DateCodec struct{}

func (DateCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (DateCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (DateCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	if _, ok := value.(DateValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanDateCodecBinary{}
	case TextFormatCode:
		return encodePlanDateCodecText{}
	}

	return nil
}

type encodePlanDateCodecBinary struct{}

func (encodePlanDateCodecBinary) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	date, err := value.(DateValuer).DateValue()
	if err != nil {
		return nil, err
	}

	if !date.Valid {
		return nil, nil
	}

	var daysSinceDateEpoch int32
	switch date.InfinityModifier {
	case None:
		tUnix := time.Date(date.Time.Year(), date.Time.Month(), date.Time.Day(), 0, 0, 0, 0, time.UTC).Unix()
		dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

		secSinceDateEpoch := tUnix - dateEpoch
		daysSinceDateEpoch = int32(secSinceDateEpoch / 86400)
	case Infinity:
		daysSinceDateEpoch = infinityDayOffset
	case NegativeInfinity:
		daysSinceDateEpoch = negativeInfinityDayOffset
	}

	return pgio.AppendInt32(buf, daysSinceDateEpoch), nil
}

type encodePlanDateCodecText struct{}

func (encodePlanDateCodecText) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	date, err := value.(DateValuer).DateValue()
	if err != nil {
		return nil, err
	}

	if !date.Valid {
		return nil, nil
	}

	var s string

	switch date.InfinityModifier {
	case None:
		s = date.Time.Format("2006-01-02")
	case Infinity:
		s = "infinity"
	case NegativeInfinity:
		s = "-infinity"
	}

	return append(buf, s...), nil
}

func (DateCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {

	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case DateScanner:
			return scanPlanBinaryDateToDateScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case DateScanner:
			return scanPlanTextAnyToDateScanner{}
		}
	}

	return nil
}

type scanPlanBinaryDateToDateScanner struct{}

func (scanPlanBinaryDateToDateScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	scanner := (dst).(DateScanner)

	if src == nil {
		return scanner.ScanDate(Date{})
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length for date: %v", len(src))
	}

	dayOffset := int32(binary.BigEndian.Uint32(src))

	switch dayOffset {
	case infinityDayOffset:
		return scanner.ScanDate(Date{InfinityModifier: Infinity, Valid: true})
	case negativeInfinityDayOffset:
		return scanner.ScanDate(Date{InfinityModifier: -Infinity, Valid: true})
	default:
		t := time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.UTC)
		return scanner.ScanDate(Date{Time: t, Valid: true})
	}
}

type scanPlanTextAnyToDateScanner struct{}

func (scanPlanTextAnyToDateScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	scanner := (dst).(DateScanner)

	if src == nil {
		return scanner.ScanDate(Date{})
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		return scanner.ScanDate(Date{InfinityModifier: Infinity, Valid: true})
	case "-infinity":
		return scanner.ScanDate(Date{InfinityModifier: -Infinity, Valid: true})
	default:
		t, err := time.ParseInLocation("2006-01-02", sbuf, time.UTC)
		if err != nil {
			return err
		}

		return scanner.ScanDate(Date{Time: t, Valid: true})
	}
}

func (c DateCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	return codecDecodeToTextFormat(c, ci, oid, format, src)
}

func (c DateCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var date Date
	err := codecScan(c, ci, oid, format, src, &date)
	if err != nil {
		return nil, err
	}

	if date.Valid {
		switch date.InfinityModifier {
		case None:
			return date.Time, nil
		case Infinity:
			return "infinity", nil
		case NegativeInfinity:
			return "-infinity", nil
		}
	}

	return nil, nil
}
