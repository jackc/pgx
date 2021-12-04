package pgtype

import (
	"fmt"
	"math"
	"strconv"
)

// QChar is for PostgreSQL's special 8-bit-only "char" type more akin to the C
// language's char type, or Go's byte type. (Note that the name in PostgreSQL
// itself is "char", in double-quotes, and not char.) It gets used a lot in
// PostgreSQL's system tables to hold a single ASCII character value (eg
// pg_class.relkind). It is named Qchar for quoted char to disambiguate from SQL
// standard type char.
//
// Not all possible values of QChar are representable in the text format.
// Therefore, QChar does not implement TextEncoder and TextDecoder. In
// addition, database/sql Scanner and database/sql/driver Value are not
// implemented.
type QChar struct {
	Int   int8
	Valid bool
}

func (dst *QChar) Set(src interface{}) error {
	if src == nil {
		*dst = QChar{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case int8:
		*dst = QChar{Int: value, Valid: true}
	case uint8:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case int16:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case uint16:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case int32:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case uint32:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case int64:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case uint64:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case int:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case uint:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Valid: true}
	case string:
		num, err := strconv.ParseInt(value, 10, 8)
		if err != nil {
			return err
		}
		*dst = QChar{Int: int8(num), Valid: true}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to QChar", value)
	}

	return nil
}

func (dst QChar) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.Int
}

func (src *QChar) AssignTo(dst interface{}) error {
	return int64AssignTo(int64(src.Int), src.Valid, dst)
}

func (dst *QChar) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = QChar{}
		return nil
	}

	if len(src) != 1 {
		return fmt.Errorf(`invalid length for "char": %v`, len(src))
	}

	*dst = QChar{Int: int8(src[0]), Valid: true}
	return nil
}

func (src QChar) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, byte(src.Int)), nil
}
