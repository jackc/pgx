package pgtype

import (
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int2 struct {
	Int    int16
	Status Status
}

func (i *Int2) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Int2:
		*i = value
	case int8:
		*i = Int2{Int: int16(value), Status: Present}
	case uint8:
		*i = Int2{Int: int16(value), Status: Present}
	case int16:
		*i = Int2{Int: int16(value), Status: Present}
	case uint16:
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case int32:
		if value < math.MinInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case uint32:
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case int64:
		if value < math.MinInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case uint64:
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case int:
		if value < math.MinInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case uint:
		if value > math.MaxInt16 {
			return fmt.Errorf("%d is greater than maximum value for Int2", value)
		}
		*i = Int2{Int: int16(value), Status: Present}
	case string:
		num, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return err
		}
		*i = Int2{Int: int16(num), Status: Present}
	default:
		if originalSrc, ok := underlyingIntType(src); ok {
			return i.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Int2", value)
	}

	return nil
}

func (i *Int2) AssignTo(dst interface{}) error {
	return nil
}

func (i *Int2) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*i = Int2{Status: Null}
		return nil
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

	*i = Int2{Int: int16(n), Status: Present}
	return nil
}

func (i *Int2) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*i = Int2{Status: Null}
		return nil
	}

	if size != 2 {
		return fmt.Errorf("invalid length for int2: %v", size)
	}

	n, err := pgio.ReadInt16(r)
	if err != nil {
		return err
	}

	*i = Int2{Int: int16(n), Status: Present}
	return nil
}

func (i Int2) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, i.Status); done {
		return err
	}

	s := strconv.FormatInt(int64(i.Int), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int2) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, i.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 2)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt16(w, i.Int)
	return err
}
