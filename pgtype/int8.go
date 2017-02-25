package pgtype

import (
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

func (i *Int8) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Int8:
		*i = value
	case int8:
		*i = Int8{Int: int64(value), Status: Present}
	case uint8:
		*i = Int8{Int: int64(value), Status: Present}
	case int16:
		*i = Int8{Int: int64(value), Status: Present}
	case uint16:
		*i = Int8{Int: int64(value), Status: Present}
	case int32:
		*i = Int8{Int: int64(value), Status: Present}
	case uint32:
		*i = Int8{Int: int64(value), Status: Present}
	case int64:
		*i = Int8{Int: int64(value), Status: Present}
	case uint64:
		if value > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		*i = Int8{Int: int64(value), Status: Present}
	case int:
		if int64(value) < math.MinInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		if int64(value) > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		*i = Int8{Int: int64(value), Status: Present}
	case uint:
		if uint64(value) > math.MaxInt64 {
			return fmt.Errorf("%d is greater than maximum value for Int8", value)
		}
		*i = Int8{Int: int64(value), Status: Present}
	case string:
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		*i = Int8{Int: num, Status: Present}
	default:
		if originalSrc, ok := underlyingIntType(src); ok {
			return i.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Int8", value)
	}

	return nil
}

func (i *Int8) AssignTo(dst interface{}) error {
	return nil
}

func (i *Int8) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*i = Int8{Status: Null}
		return nil
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

	*i = Int8{Int: n, Status: Present}
	return nil
}

func (i *Int8) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*i = Int8{Status: Null}
		return nil
	}

	if size != 8 {
		return fmt.Errorf("invalid length for int8: %v", size)
	}

	n, err := pgio.ReadInt64(r)
	if err != nil {
		return err
	}

	*i = Int8{Int: n, Status: Present}
	return nil
}

func (i Int8) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, i.Status); done {
		return err
	}

	s := strconv.FormatInt(i.Int, 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int8) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, i.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 8)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt64(w, i.Int)
	return err
}
