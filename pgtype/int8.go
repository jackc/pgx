package pgtype

import (
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int8 int64

func ConvertToInt8(src interface{}) (Int8, error) {
	switch value := src.(type) {
	case Int8:
		return value, nil
	case int8:
		return Int8(value), nil
	case uint8:
		return Int8(value), nil
	case int16:
		return Int8(value), nil
	case uint16:
		return Int8(value), nil
	case int32:
		return Int8(value), nil
	case uint32:
		return Int8(value), nil
	case int64:
		return Int8(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		return Int8(value), nil
	case int:
		if int64(value) < math.MinInt64 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		if int64(value) > math.MaxInt64 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		return Int8(value), nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return 0, fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		return Int8(value), nil
	case string:
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return Int8(num), nil
	default:
		if originalSrc, ok := underlyingIntType(src); ok {
			return ConvertToInt8(originalSrc)
		}
		return 0, fmt.Errorf("cannot convert %v to Int8", value)
	}
}

func (i *Int8) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int8: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return err
	}

	*i = Int8(n)
	return nil
}

func (i *Int8) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 8 {
		return fmt.Errorf("invalid length for int8: %v", size)
	}

	n, err := pgio.ReadInt64(r)
	if err != nil {
		return err
	}

	*i = Int8(n)
	return nil
}

func (i Int8) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int8) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 8)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt64(w, int64(i))
	return err
}
