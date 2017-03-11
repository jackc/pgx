package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Float8 struct {
	Float  float64
	Status Status
}

func (dst *Float8) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Float8:
		*dst = value
	case float32:
		*dst = Float8{Float: float64(value), Status: Present}
	case float64:
		*dst = Float8{Float: value, Status: Present}
	case int8:
		*dst = Float8{Float: float64(value), Status: Present}
	case uint8:
		*dst = Float8{Float: float64(value), Status: Present}
	case int16:
		*dst = Float8{Float: float64(value), Status: Present}
	case uint16:
		*dst = Float8{Float: float64(value), Status: Present}
	case int32:
		*dst = Float8{Float: float64(value), Status: Present}
	case uint32:
		*dst = Float8{Float: float64(value), Status: Present}
	case int64:
		f64 := float64(value)
		if int64(f64) == value {
			*dst = Float8{Float: f64, Status: Present}
		} else {
			return fmt.Errorf("%v cannot be exactly represented as float64", value)
		}
	case uint64:
		f64 := float64(value)
		if uint64(f64) == value {
			*dst = Float8{Float: f64, Status: Present}
		} else {
			return fmt.Errorf("%v cannot be exactly represented as float64", value)
		}
	case int:
		f64 := float64(value)
		if int(f64) == value {
			*dst = Float8{Float: f64, Status: Present}
		} else {
			return fmt.Errorf("%v cannot be exactly represented as float64", value)
		}
	case uint:
		f64 := float64(value)
		if uint(f64) == value {
			*dst = Float8{Float: f64, Status: Present}
		} else {
			return fmt.Errorf("%v cannot be exactly represented as float64", value)
		}
	case string:
		num, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		*dst = Float8{Float: float64(num), Status: Present}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Float8", value)
	}

	return nil
}

func (src *Float8) AssignTo(dst interface{}) error {
	return float64AssignTo(src.Float, src.Status, dst)
}

func (dst *Float8) DecodeText(src []byte) error {
	if src == nil {
		*dst = Float8{Status: Null}
		return nil
	}

	n, err := strconv.ParseFloat(string(src), 64)
	if err != nil {
		return err
	}

	*dst = Float8{Float: n, Status: Present}
	return nil
}

func (dst *Float8) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Float8{Status: Null}
		return nil
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for float4: %v", len(src))
	}

	n := int64(binary.BigEndian.Uint64(src))

	*dst = Float8{Float: math.Float64frombits(uint64(n)), Status: Present}
	return nil
}

func (src Float8) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, strconv.FormatFloat(float64(src.Float), 'f', -1, 64))
	return false, err
}

func (src Float8) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteInt64(w, int64(math.Float64bits(src.Float)))
	return false, err
}
