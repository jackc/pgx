package pgtype

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

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
