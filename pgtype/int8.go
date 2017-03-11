package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int8 struct {
	Int    int64
	Status Status
}

func (dst *Int8) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Int8:
		*dst = value
	case int8:
		*dst = Int8{Int: int64(value), Status: Present}
	case uint8:
		*dst = Int8{Int: int64(value), Status: Present}
	case int16:
		*dst = Int8{Int: int64(value), Status: Present}
	case uint16:
		*dst = Int8{Int: int64(value), Status: Present}
	case int32:
		*dst = Int8{Int: int64(value), Status: Present}
	case uint32:
		*dst = Int8{Int: int64(value), Status: Present}
	case int64:
		*dst = Int8{Int: int64(value), Status: Present}
	case uint64:
		if value > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		*dst = Int8{Int: int64(value), Status: Present}
	case int:
		if int64(value) < math.MinInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		if int64(value) > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		*dst = Int8{Int: int64(value), Status: Present}
	case uint:
		if uint64(value) > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		*dst = Int8{Int: int64(value), Status: Present}
	case string:
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		*dst = Int8{Int: num, Status: Present}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Int8", value)
	}

	return nil
}

func (src *Int8) AssignTo(dst interface{}) error {
	return int64AssignTo(int64(src.Int), src.Status, dst)
}

func (dst *Int8) DecodeText(src []byte) error {
	if src == nil {
		*dst = Int8{Status: Null}
		return nil
	}

	n, err := strconv.ParseInt(string(src), 10, 64)
	if err != nil {
		return err
	}

	*dst = Int8{Int: n, Status: Present}
	return nil
}

func (dst *Int8) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Int8{Status: Null}
		return nil
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for int8: %v", len(src))
	}

	n := int64(binary.BigEndian.Uint64(src))

	*dst = Int8{Int: n, Status: Present}
	return nil
}

func (src Int8) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, strconv.FormatInt(src.Int, 10))
	return false, err
}

func (src Int8) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteInt64(w, src.Int)
	return false, err
}
