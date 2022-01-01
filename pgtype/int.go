// Do not edit. Generated from pgtype/int.go.erb
package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/jackc/pgio"
)

type Int64Scanner interface {
	ScanInt64(v int64, valid bool) error
}

type Int2 struct {
	Int   int16
	Valid bool
}

// ScanInt64 implements the Int64Scanner interface.
func (dst *Int2) ScanInt64(n int64, valid bool) error {
	if !valid {
		*dst = Int2{}
		return nil
	}

	if n < math.MinInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	if n > math.MaxInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	*dst = Int2{Int: int16(n), Valid: true}

	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Int2) Scan(src interface{}) error {
	if src == nil {
		*dst = Int2{}
		return nil
	}

	var n int64

	switch src := src.(type) {
	case int64:
		n = src
	case string:
		var err error
		n, err = strconv.ParseInt(src, 10, 16)
		if err != nil {
			return err
		}
	case []byte:
		var err error
		n, err = strconv.ParseInt(string(src), 10, 16)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot scan %T", src)
	}

	if n < math.MinInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	if n > math.MaxInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	*dst = Int2{Int: int16(n), Valid: true}

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int2) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return int64(src.Int), nil
}

func (src Int2) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}
	return []byte(strconv.FormatInt(int64(src.Int), 10)), nil
}

type Int2Codec struct{}

func (Int2Codec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (Int2Codec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (Int2Codec) Encode(ci *ConnInfo, oid uint32, format int16, value interface{}, buf []byte) (newBuf []byte, err error) {
	n, valid, err := convertToInt64ForEncode(value)
	if err != nil {
		return nil, fmt.Errorf("cannot convert %v to int2: %v", value, err)
	}
	if !valid {
		return nil, nil
	}

	if n > math.MaxInt16 {
		return nil, fmt.Errorf("%d is greater than maximum value for int2", n)
	}
	if n < math.MinInt16 {
		return nil, fmt.Errorf("%d is less than minimum value for int2", n)
	}

	switch format {
	case BinaryFormatCode:
		return pgio.AppendInt16(buf, int16(n)), nil
	case TextFormatCode:
		return append(buf, strconv.FormatInt(n, 10)...), nil
	default:
		return nil, fmt.Errorf("unknown format code: %v", format)
	}
}

func (Int2Codec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {

	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case *int8:
			return scanPlanBinaryInt2ToInt8{}
		case *int16:
			return scanPlanBinaryInt2ToInt16{}
		case *int32:
			return scanPlanBinaryInt2ToInt32{}
		case *int64:
			return scanPlanBinaryInt2ToInt64{}
		case *int:
			return scanPlanBinaryInt2ToInt{}
		case *uint8:
			return scanPlanBinaryInt2ToUint8{}
		case *uint16:
			return scanPlanBinaryInt2ToUint16{}
		case *uint32:
			return scanPlanBinaryInt2ToUint32{}
		case *uint64:
			return scanPlanBinaryInt2ToUint64{}
		case *uint:
			return scanPlanBinaryInt2ToUint{}
		case Int64Scanner:
			return scanPlanBinaryInt2ToInt64Scanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case *int8:
			return scanPlanTextAnyToInt8{}
		case *int16:
			return scanPlanTextAnyToInt16{}
		case *int32:
			return scanPlanTextAnyToInt32{}
		case *int64:
			return scanPlanTextAnyToInt64{}
		case *int:
			return scanPlanTextAnyToInt{}
		case *uint8:
			return scanPlanTextAnyToUint8{}
		case *uint16:
			return scanPlanTextAnyToUint16{}
		case *uint32:
			return scanPlanTextAnyToUint32{}
		case *uint64:
			return scanPlanTextAnyToUint64{}
		case *uint:
			return scanPlanTextAnyToUint{}
		case Int64Scanner:
			return scanPlanTextAnyToInt64Scanner{}
		}
	}

	return nil
}

func (c Int2Codec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var n int64
	scanPlan := c.PlanScan(ci, oid, format, &n, true)
	if scanPlan == nil {
		return nil, fmt.Errorf("PlanScan did not find a plan")
	}
	err := scanPlan.Scan(ci, oid, format, src, &n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (c Int2Codec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var n int16
	scanPlan := c.PlanScan(ci, oid, format, &n, true)
	if scanPlan == nil {
		return nil, fmt.Errorf("PlanScan did not find a plan")
	}
	err := scanPlan.Scan(ci, oid, format, src, &n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

type scanPlanBinaryInt2ToInt8 struct{}

func (scanPlanBinaryInt2ToInt8) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for int2: %v", len(src))
	}

	p, ok := (dst).(*int8)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n := int16(binary.BigEndian.Uint16(src))
	if n < math.MinInt8 {
		return fmt.Errorf("%d is less than minimum value for int8", n)
	} else if n > math.MaxInt8 {
		return fmt.Errorf("%d is greater than maximum value for int8", n)
	}

	*p = int8(n)

	return nil
}

type scanPlanBinaryInt2ToUint8 struct{}

func (scanPlanBinaryInt2ToUint8) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for uint2: %v", len(src))
	}

	p, ok := (dst).(*uint8)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n := int16(binary.BigEndian.Uint16(src))
	if n < 0 {
		return fmt.Errorf("%d is less than minimum value for uint8", n)
	}

	if n > math.MaxUint8 {
		return fmt.Errorf("%d is greater than maximum value for uint8", n)
	}

	*p = uint8(n)

	return nil
}

type scanPlanBinaryInt2ToInt16 struct{}

func (scanPlanBinaryInt2ToInt16) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for int2: %v", len(src))
	}

	p, ok := (dst).(*int16)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	*p = int16(binary.BigEndian.Uint16(src))

	return nil
}

type scanPlanBinaryInt2ToUint16 struct{}

func (scanPlanBinaryInt2ToUint16) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for uint2: %v", len(src))
	}

	p, ok := (dst).(*uint16)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n := int16(binary.BigEndian.Uint16(src))
	if n < 0 {
		return fmt.Errorf("%d is less than minimum value for uint16", n)
	}

	*p = uint16(n)

	return nil
}

type scanPlanBinaryInt2ToInt32 struct{}

func (scanPlanBinaryInt2ToInt32) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for int2: %v", len(src))
	}

	p, ok := (dst).(*int32)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	*p = int32(binary.BigEndian.Uint16(src))

	return nil
}

type scanPlanBinaryInt2ToUint32 struct{}

func (scanPlanBinaryInt2ToUint32) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for uint2: %v", len(src))
	}

	p, ok := (dst).(*uint32)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n := int16(binary.BigEndian.Uint16(src))
	if n < 0 {
		return fmt.Errorf("%d is less than minimum value for uint32", n)
	}

	*p = uint32(n)

	return nil
}

type scanPlanBinaryInt2ToInt64 struct{}

func (scanPlanBinaryInt2ToInt64) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for int2: %v", len(src))
	}

	p, ok := (dst).(*int64)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	*p = int64(binary.BigEndian.Uint16(src))

	return nil
}

type scanPlanBinaryInt2ToUint64 struct{}

func (scanPlanBinaryInt2ToUint64) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for uint2: %v", len(src))
	}

	p, ok := (dst).(*uint64)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n := int16(binary.BigEndian.Uint16(src))
	if n < 0 {
		return fmt.Errorf("%d is less than minimum value for uint64", n)
	}

	*p = uint64(n)

	return nil
}

type scanPlanBinaryInt2ToInt struct{}

func (scanPlanBinaryInt2ToInt) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for int2: %v", len(src))
	}

	p, ok := (dst).(*int)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	*p = int(binary.BigEndian.Uint16(src))

	return nil
}

type scanPlanBinaryInt2ToUint struct{}

func (scanPlanBinaryInt2ToUint) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for uint2: %v", len(src))
	}

	p, ok := (dst).(*uint)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n := int64(binary.BigEndian.Uint16(src))
	if n < 0 {
		return fmt.Errorf("%d is less than minimum value for uint", n)
	}

	*p = uint(n)

	return nil
}

type scanPlanBinaryInt2ToInt64Scanner struct{}

func (scanPlanBinaryInt2ToInt64Scanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	s, ok := (dst).(Int64Scanner)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	if src == nil {
		return s.ScanInt64(0, false)
	}

	if len(src) != 2 {
		return fmt.Errorf("invalid length for int2: %v", len(src))
	}

	n := int64(binary.BigEndian.Uint16(src))

	return s.ScanInt64(n, true)
}

type scanPlanTextAnyToInt8 struct{}

func (scanPlanTextAnyToInt8) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*int8)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseInt(string(src), 10, 8)
	if err != nil {
		return err
	}

	*p = int8(n)
	return nil
}

type scanPlanTextAnyToUint8 struct{}

func (scanPlanTextAnyToUint8) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*uint8)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseUint(string(src), 10, 8)
	if err != nil {
		return err
	}

	*p = uint8(n)
	return nil
}

type scanPlanTextAnyToInt16 struct{}

func (scanPlanTextAnyToInt16) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*int16)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseInt(string(src), 10, 16)
	if err != nil {
		return err
	}

	*p = int16(n)
	return nil
}

type scanPlanTextAnyToUint16 struct{}

func (scanPlanTextAnyToUint16) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*uint16)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseUint(string(src), 10, 16)
	if err != nil {
		return err
	}

	*p = uint16(n)
	return nil
}

type scanPlanTextAnyToInt32 struct{}

func (scanPlanTextAnyToInt32) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*int32)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseInt(string(src), 10, 32)
	if err != nil {
		return err
	}

	*p = int32(n)
	return nil
}

type scanPlanTextAnyToUint32 struct{}

func (scanPlanTextAnyToUint32) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*uint32)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseUint(string(src), 10, 32)
	if err != nil {
		return err
	}

	*p = uint32(n)
	return nil
}

type scanPlanTextAnyToInt64 struct{}

func (scanPlanTextAnyToInt64) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*int64)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseInt(string(src), 10, 64)
	if err != nil {
		return err
	}

	*p = int64(n)
	return nil
}

type scanPlanTextAnyToUint64 struct{}

func (scanPlanTextAnyToUint64) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*uint64)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseUint(string(src), 10, 64)
	if err != nil {
		return err
	}

	*p = uint64(n)
	return nil
}

type scanPlanTextAnyToInt struct{}

func (scanPlanTextAnyToInt) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*int)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseInt(string(src), 10, 0)
	if err != nil {
		return err
	}

	*p = int(n)
	return nil
}

type scanPlanTextAnyToUint struct{}

func (scanPlanTextAnyToUint) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p, ok := (dst).(*uint)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseUint(string(src), 10, 0)
	if err != nil {
		return err
	}

	*p = uint(n)
	return nil
}

type scanPlanTextAnyToInt64Scanner struct{}

func (scanPlanTextAnyToInt64Scanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	s, ok := (dst).(Int64Scanner)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	if src == nil {
		return s.ScanInt64(0, false)
	}

	n, err := strconv.ParseInt(string(src), 10, 64)
	if err != nil {
		return err
	}

	err = s.ScanInt64(n, true)
	if err != nil {
		return err
	}

	return nil
}
