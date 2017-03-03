package pgtype

import (
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

func (i *Int4) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Int4:
		*i = value
	case int8:
		*i = Int4{Int: int32(value), Status: Present}
	case uint8:
		*i = Int4{Int: int32(value), Status: Present}
	case int16:
		*i = Int4{Int: int32(value), Status: Present}
	case uint16:
		*i = Int4{Int: int32(value), Status: Present}
	case int32:
		*i = Int4{Int: int32(value), Status: Present}
	case uint32:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*i = Int4{Int: int32(value), Status: Present}
	case int64:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*i = Int4{Int: int32(value), Status: Present}
	case uint64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*i = Int4{Int: int32(value), Status: Present}
	case int:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*i = Int4{Int: int32(value), Status: Present}
	case uint:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*i = Int4{Int: int32(value), Status: Present}
	case string:
		num, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		*i = Int4{Int: int32(num), Status: Present}
	default:
		if originalSrc, ok := underlyingIntType(src); ok {
			return i.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Int8", value)
	}

	return nil
}

func (i *Int4) AssignTo(dst interface{}) error {
	return int64AssignTo(int64(i.Int), i.Status, dst)
}

func (i *Int4) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*i = Int4{Status: Null}
		return nil
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

	*i = Int4{Int: int32(n), Status: Present}
	return nil
}

func (i *Int4) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*i = Int4{Status: Null}
		return nil
	}

	if size != 4 {
		return fmt.Errorf("invalid length for int4: %v", size)
	}

	n, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	*i = Int4{Int: n, Status: Present}
	return nil
}

func (i Int4) EncodeText(w io.Writer) error {
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

func (i Int4) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, i.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, i.Int)
	return err
}
