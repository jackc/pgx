package pgtype

import (
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

// QChar is for PostgreSQL's special 8-bit-only "char" type more akin to the C
// language's char type, or Go's byte type. (Note that the name in PostgreSQL
// itself is "char", in double-quotes, and not char.) It gets used a lot in
// PostgreSQL's system tables to hold a single ASCII character value (eg
// pg_class.relkind). It is named Qchar for quoted char to disambiguate from SQL
// standard type char.
//
// Not all possible values of QChar are representable in the text format.
// Therefore, QChar does not implement TextEncoder and TextDecoder.
type QChar struct {
	Int    int8
	Status Status
}

func (dst *QChar) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case QChar:
		*dst = value
	case int8:
		*dst = QChar{Int: value, Status: Present}
	case uint8:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case int16:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case uint16:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case int32:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case uint32:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case int64:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case uint64:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case int:
		if value < math.MinInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case uint:
		if value > math.MaxInt8 {
			return fmt.Errorf("%d is greater than maximum value for QChar", value)
		}
		*dst = QChar{Int: int8(value), Status: Present}
	case string:
		num, err := strconv.ParseInt(value, 10, 8)
		if err != nil {
			return err
		}
		*dst = QChar{Int: int8(num), Status: Present}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to QChar", value)
	}

	return nil
}

func (src *QChar) AssignTo(dst interface{}) error {
	return int64AssignTo(int64(src.Int), src.Status, dst)
}

func (dst *QChar) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = QChar{Status: Null}
		return nil
	}

	if len(src) != 1 {
		return fmt.Errorf(`invalid length for "char": %v`, len(src))
	}

	*dst = QChar{Int: int8(src[0]), Status: Present}
	return nil
}

func (src QChar) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 1)
	if err != nil {
		return nil
	}

	return pgio.WriteByte(w, byte(src.Int))
}
