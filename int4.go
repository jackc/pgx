package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int4 struct {
	Int    int32
	Status Status
}

func (dst *Int4) Set(src interface{}) error {
	switch value := src.(type) {
	case int8:
		*dst = Int4{Int: int32(value), Status: Present}
	case uint8:
		*dst = Int4{Int: int32(value), Status: Present}
	case int16:
		*dst = Int4{Int: int32(value), Status: Present}
	case uint16:
		*dst = Int4{Int: int32(value), Status: Present}
	case int32:
		*dst = Int4{Int: int32(value), Status: Present}
	case uint32:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Status: Present}
	case int64:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Status: Present}
	case uint64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Status: Present}
	case int:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Status: Present}
	case uint:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Status: Present}
	case string:
		num, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		*dst = Int4{Int: int32(num), Status: Present}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Int8", value)
	}

	return nil
}

func (dst *Int4) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Int
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Int4) AssignTo(dst interface{}) error {
	return int64AssignTo(int64(src.Int), src.Status, dst)
}

func (dst *Int4) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Int4{Status: Null}
		return nil
	}

	n, err := strconv.ParseInt(string(src), 10, 32)
	if err != nil {
		return err
	}

	*dst = Int4{Int: int32(n), Status: Present}
	return nil
}

func (dst *Int4) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Int4{Status: Null}
		return nil
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length for int4: %v", len(src))
	}

	n := int32(binary.BigEndian.Uint32(src))
	*dst = Int4{Int: n, Status: Present}
	return nil
}

func (src Int4) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, strconv.FormatInt(int64(src.Int), 10))
	return false, err
}

func (src Int4) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteInt32(w, src.Int)
	return false, err
}
