package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/jackc/pgio"
)

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
