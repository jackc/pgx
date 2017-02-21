package pgtype

import (
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int4 int32

func ConvertToInt4(src interface{}) (Int4, error) {
	switch value := src.(type) {
	case Int4:
		return value, nil
	case int8:
		return Int4(value), nil
	case uint8:
		return Int4(value), nil
	case int16:
		return Int4(value), nil
	case uint16:
		return Int4(value), nil
	case int32:
		return Int4(value), nil
	case uint32:
		if value > math.MaxInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		return Int4(value), nil
	case int64:
		if value < math.MinInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		return Int4(value), nil
	case uint64:
		if value > math.MaxInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		return Int4(value), nil
	case int:
		if value < math.MinInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		return Int4(value), nil
	case uint:
		if value > math.MaxInt32 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		return Int4(value), nil
	case string:
		num, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return 0, err
		}
		return Int4(num), nil
	default:
		if originalSrc, ok := underlyingIntType(src); ok {
			return ConvertToInt4(originalSrc)
		}
		return 0, fmt.Errorf("cannot convert %v to Int8", value)
	}
}

func (i *Int4) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int4: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(string(buf), 10, 32)
	if err != nil {
		return err
	}

	*i = Int4(n)
	return nil
}

func (i *Int4) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 4 {
		return fmt.Errorf("invalid length for int4: %v", size)
	}

	n, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	*i = Int4(n)
	return nil
}

func (i Int4) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int4) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, int32(i))
	return err
}
