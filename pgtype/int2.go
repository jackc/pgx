package pgtype

import (
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int2 int16

func ConvertToInt2(src interface{}) (Int2, error) {
	switch value := src.(type) {
	case Int2:
		return value, nil
	case int8:
		return Int2(value), nil
	case uint8:
		return Int2(value), nil
	case int16:
		return Int2(value), nil
	case uint16:
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case int32:
		if value < math.MinInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case uint32:
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case int64:
		if value < math.MinInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case uint64:
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case int:
		if value < math.MinInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case uint:
		if value > math.MaxInt16 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		return Int2(value), nil
	case string:
		num, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return 0, err
		}
		return Int2(num), nil
	default:
		if originalSrc, ok := underlyingIntType(src); ok {
			return ConvertToInt2(originalSrc)
		}
		return 0, fmt.Errorf("cannot convert %v to Int2", value)
	}
}

func (i *Int2) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int2: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(string(buf), 10, 16)
	if err != nil {
		return err
	}

	*i = Int2(n)
	return nil
}

func (i *Int2) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 2 {
		return fmt.Errorf("invalid length for int2: %v", size)
	}

	n, err := pgio.ReadInt16(r)
	if err != nil {
		return err
	}

	*i = Int2(n)
	return nil
}

func (i Int2) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int2) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 2)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt16(w, int16(i))
	return err
}
