package pgtype

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"time"
)

type int8Wrapper int8

func (w int8Wrapper) SkipUnderlyingTypePlan() {}

func (w *int8Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int8")
	}

	if v.Int < math.MinInt8 {
		return fmt.Errorf("%d is less than minimum value for int8", v.Int)
	}
	if v.Int > math.MaxInt8 {
		return fmt.Errorf("%d is greater than maximum value for int8", v.Int)
	}
	*w = int8Wrapper(v.Int)

	return nil
}

func (w int8Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type int16Wrapper int16

func (w int16Wrapper) SkipUnderlyingTypePlan() {}

func (w *int16Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int16")
	}

	if v.Int < math.MinInt16 {
		return fmt.Errorf("%d is less than minimum value for int16", v.Int)
	}
	if v.Int > math.MaxInt16 {
		return fmt.Errorf("%d is greater than maximum value for int16", v.Int)
	}
	*w = int16Wrapper(v.Int)

	return nil
}

func (w int16Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type int32Wrapper int32

func (w int32Wrapper) SkipUnderlyingTypePlan() {}

func (w *int32Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int32")
	}

	if v.Int < math.MinInt32 {
		return fmt.Errorf("%d is less than minimum value for int32", v.Int)
	}
	if v.Int > math.MaxInt32 {
		return fmt.Errorf("%d is greater than maximum value for int32", v.Int)
	}
	*w = int32Wrapper(v.Int)

	return nil
}

func (w int32Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type int64Wrapper int64

func (w int64Wrapper) SkipUnderlyingTypePlan() {}

func (w *int64Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int64")
	}

	*w = int64Wrapper(v.Int)

	return nil
}

func (w int64Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type intWrapper int

func (w intWrapper) SkipUnderlyingTypePlan() {}

func (w *intWrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int")
	}

	if v.Int < math.MinInt {
		return fmt.Errorf("%d is less than minimum value for int", v.Int)
	}
	if v.Int > math.MaxInt {
		return fmt.Errorf("%d is greater than maximum value for int", v.Int)
	}

	*w = intWrapper(v.Int)

	return nil
}

func (w intWrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type uint8Wrapper uint8

func (w uint8Wrapper) SkipUnderlyingTypePlan() {}

func (w *uint8Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint8")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint8", v.Int)
	}
	if v.Int > math.MaxUint8 {
		return fmt.Errorf("%d is greater than maximum value for uint8", v.Int)
	}
	*w = uint8Wrapper(v.Int)

	return nil
}

func (w uint8Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type uint16Wrapper uint16

func (w uint16Wrapper) SkipUnderlyingTypePlan() {}

func (w *uint16Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint16")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint16", v.Int)
	}
	if v.Int > math.MaxUint16 {
		return fmt.Errorf("%d is greater than maximum value for uint16", v.Int)
	}
	*w = uint16Wrapper(v.Int)

	return nil
}

func (w uint16Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type uint32Wrapper uint32

func (w uint32Wrapper) SkipUnderlyingTypePlan() {}

func (w *uint32Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint32")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint32", v.Int)
	}
	if v.Int > math.MaxUint32 {
		return fmt.Errorf("%d is greater than maximum value for uint32", v.Int)
	}
	*w = uint32Wrapper(v.Int)

	return nil
}

func (w uint32Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(w), Valid: true}, nil
}

type uint64Wrapper uint64

func (w uint64Wrapper) SkipUnderlyingTypePlan() {}

func (w *uint64Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint64")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint64", v.Int)
	}

	*w = uint64Wrapper(v.Int)

	return nil
}

func (w uint64Wrapper) Int64Value() (Int8, error) {
	if uint64(w) > uint64(math.MaxInt64) {
		return Int8{}, fmt.Errorf("%d is greater than maximum value for int64", w)
	}

	return Int8{Int: int64(w), Valid: true}, nil
}

type uintWrapper uint

func (w uintWrapper) SkipUnderlyingTypePlan() {}

func (w *uintWrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint64")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint64", v.Int)
	}

	if uint64(v.Int) > math.MaxUint {
		return fmt.Errorf("%d is greater than maximum value for uint", v.Int)
	}

	*w = uintWrapper(v.Int)

	return nil
}

func (w uintWrapper) Int64Value() (Int8, error) {
	if uint64(w) > uint64(math.MaxInt64) {
		return Int8{}, fmt.Errorf("%d is greater than maximum value for int64", w)
	}

	return Int8{Int: int64(w), Valid: true}, nil
}

type float32Wrapper float32

func (w float32Wrapper) SkipUnderlyingTypePlan() {}

func (w *float32Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *float32")
	}

	*w = float32Wrapper(v.Int)

	return nil
}

func (w float32Wrapper) Int64Value() (Int8, error) {
	if w > math.MaxInt64 {
		return Int8{}, fmt.Errorf("%f is greater than maximum value for int64", w)
	}

	return Int8{Int: int64(w), Valid: true}, nil
}

func (w *float32Wrapper) ScanFloat64(v Float8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *float32")
	}

	*w = float32Wrapper(v.Float)

	return nil
}

func (w float32Wrapper) Float64Value() (Float8, error) {
	return Float8{Float: float64(w), Valid: true}, nil
}

type float64Wrapper float64

func (w float64Wrapper) SkipUnderlyingTypePlan() {}

func (w *float64Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *float64")
	}

	*w = float64Wrapper(v.Int)

	return nil
}

func (w float64Wrapper) Int64Value() (Int8, error) {
	if w > math.MaxInt64 {
		return Int8{}, fmt.Errorf("%f is greater than maximum value for int64", w)
	}

	return Int8{Int: int64(w), Valid: true}, nil
}

func (w *float64Wrapper) ScanFloat64(v Float8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *float64")
	}

	*w = float64Wrapper(v.Float)

	return nil
}

func (w float64Wrapper) Float64Value() (Float8, error) {
	return Float8{Float: float64(w), Valid: true}, nil
}

type stringWrapper string

func (w stringWrapper) SkipUnderlyingTypePlan() {}

func (w *stringWrapper) ScanText(v Text) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *string")
	}

	*w = stringWrapper(v.String)
	return nil
}

func (w stringWrapper) TextValue() (Text, error) {
	return Text{String: string(w), Valid: true}, nil
}

func (w *stringWrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *string")
	}

	*w = stringWrapper(strconv.FormatInt(v.Int, 10))

	return nil
}

func (w stringWrapper) Int64Value() (Int8, error) {
	num, err := strconv.ParseInt(string(w), 10, 64)
	if err != nil {
		return Int8{}, err
	}

	return Int8{Int: int64(num), Valid: true}, nil
}

type timeWrapper time.Time

func (w *timeWrapper) ScanDate(v Date) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *time.Time")
	}

	switch v.InfinityModifier {
	case None:
		*w = timeWrapper(v.Time)
		return nil
	case Infinity:
		return fmt.Errorf("cannot scan Infinity into *time.Time")
	case NegativeInfinity:
		return fmt.Errorf("cannot scan -Infinity into *time.Time")
	default:
		return fmt.Errorf("invalid InfinityModifier: %v", v.InfinityModifier)
	}
}

func (w timeWrapper) DateValue() (Date, error) {
	return Date{Time: time.Time(w), Valid: true}, nil
}

func (w *timeWrapper) ScanTimestamp(v Timestamp) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *time.Time")
	}

	switch v.InfinityModifier {
	case None:
		*w = timeWrapper(v.Time)
		return nil
	case Infinity:
		return fmt.Errorf("cannot scan Infinity into *time.Time")
	case NegativeInfinity:
		return fmt.Errorf("cannot scan -Infinity into *time.Time")
	default:
		return fmt.Errorf("invalid InfinityModifier: %v", v.InfinityModifier)
	}
}

func (w timeWrapper) TimestampValue() (Timestamp, error) {
	return Timestamp{Time: time.Time(w), Valid: true}, nil
}

func (w *timeWrapper) ScanTimestamptz(v Timestamptz) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *time.Time")
	}

	switch v.InfinityModifier {
	case None:
		*w = timeWrapper(v.Time)
		return nil
	case Infinity:
		return fmt.Errorf("cannot scan Infinity into *time.Time")
	case NegativeInfinity:
		return fmt.Errorf("cannot scan -Infinity into *time.Time")
	default:
		return fmt.Errorf("invalid InfinityModifier: %v", v.InfinityModifier)
	}
}

func (w timeWrapper) TimestamptzValue() (Timestamptz, error) {
	return Timestamptz{Time: time.Time(w), Valid: true}, nil
}

func (w *timeWrapper) ScanTime(v Time) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *time.Time")
	}

	// 24:00:00 is max allowed time in PostgreSQL, but time.Time will normalize that to 00:00:00 the next day.
	var maxRepresentableByTime int64 = 24*60*60*1000000 - 1
	if v.Microseconds > maxRepresentableByTime {
		return fmt.Errorf("%d microseconds cannot be represented as time.Time", v.Microseconds)
	}

	usec := v.Microseconds
	hours := usec / microsecondsPerHour
	usec -= hours * microsecondsPerHour
	minutes := usec / microsecondsPerMinute
	usec -= minutes * microsecondsPerMinute
	seconds := usec / microsecondsPerSecond
	usec -= seconds * microsecondsPerSecond
	ns := usec * 1000
	*w = timeWrapper(time.Date(2000, 1, 1, int(hours), int(minutes), int(seconds), int(ns), time.UTC))
	return nil
}

func (w timeWrapper) TimeValue() (Time, error) {
	t := time.Time(w)
	usec := int64(t.Hour())*microsecondsPerHour +
		int64(t.Minute())*microsecondsPerMinute +
		int64(t.Second())*microsecondsPerSecond +
		int64(t.Nanosecond())/1000
	return Time{Microseconds: usec, Valid: true}, nil
}

type durationWrapper time.Duration

func (w *durationWrapper) ScanInterval(v Interval) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *time.Interval")
	}

	us := int64(v.Months)*microsecondsPerMonth + int64(v.Days)*microsecondsPerDay + v.Microseconds
	*w = durationWrapper(time.Duration(us) * time.Microsecond)
	return nil
}

func (w durationWrapper) IntervalValue() (Interval, error) {
	return Interval{Microseconds: int64(w) / 1000, Valid: true}, nil
}

type netIPNetWrapper net.IPNet

func (w *netIPNetWrapper) ScanInet(v Inet) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *net.IPNet")
	}

	*w = (netIPNetWrapper)(*v.IPNet)
	return nil
}

func (w netIPNetWrapper) InetValue() (Inet, error) {
	return Inet{IPNet: (*net.IPNet)(&w), Valid: true}, nil
}

type netIPWrapper net.IP

func (w *netIPWrapper) ScanInet(v Inet) error {
	if !v.Valid {
		*w = nil
		return nil
	}

	if oneCount, bitCount := v.IPNet.Mask.Size(); oneCount != bitCount {
		return fmt.Errorf("cannot scan %v to *net.IP", v)
	}
	*w = netIPWrapper(v.IPNet.IP)
	return nil
}

func (w netIPWrapper) InetValue() (Inet, error) {
	if w == nil {
		return Inet{}, nil
	}

	bitCount := len(w) * 8
	mask := net.CIDRMask(bitCount, bitCount)
	return Inet{IPNet: &net.IPNet{Mask: mask, IP: net.IP(w)}, Valid: true}, nil
}

type mapStringToPointerStringWrapper map[string]*string

func (w *mapStringToPointerStringWrapper) ScanHstore(v Hstore) error {
	*w = mapStringToPointerStringWrapper(v)
	return nil
}

func (w mapStringToPointerStringWrapper) HstoreValue() (Hstore, error) {
	return Hstore(w), nil
}

type mapStringToStringWrapper map[string]string

func (w *mapStringToStringWrapper) ScanHstore(v Hstore) error {
	*w = make(mapStringToStringWrapper, len(v))
	for k, v := range v {
		if v == nil {
			return fmt.Errorf("cannot scan NULL to string")
		}
		(*w)[k] = *v
	}
	return nil
}

func (w mapStringToStringWrapper) HstoreValue() (Hstore, error) {
	if w == nil {
		return nil, nil
	}

	hstore := make(Hstore, len(w))
	for k, v := range w {
		s := v
		hstore[k] = &s
	}
	return hstore, nil
}

type fmtStringerWrapper struct {
	s fmt.Stringer
}

func (w fmtStringerWrapper) TextValue() (Text, error) {
	return Text{String: w.s.String(), Valid: true}, nil
}
