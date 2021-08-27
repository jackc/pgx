package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/jackc/pgio"
)

type Int4 struct {
	Int   int32
	Valid bool
}

func (dst *Int4) Set(src interface{}) error {
	if src == nil {
		*dst = Int4{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case int8:
		*dst = Int4{Int: int32(value), Valid: true}
	case uint8:
		*dst = Int4{Int: int32(value), Valid: true}
	case int16:
		*dst = Int4{Int: int32(value), Valid: true}
	case uint16:
		*dst = Int4{Int: int32(value), Valid: true}
	case int32:
		*dst = Int4{Int: int32(value), Valid: true}
	case uint32:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case int64:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case uint64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case int:
		if value < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case uint:
		if value > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case string:
		num, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		*dst = Int4{Int: int32(num), Valid: true}
	case float32:
		if value > math.MaxInt32 {
			return fmt.Errorf("%f is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case float64:
		if value > math.MaxInt32 {
			return fmt.Errorf("%f is greater than maximum value for Int4", value)
		}
		*dst = Int4{Int: int32(value), Valid: true}
	case *int8:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *uint8:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *int16:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *uint16:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *int32:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *uint32:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *int64:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *uint64:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *int:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *uint:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *string:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *float32:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	case *float64:
		if value == nil {
			*dst = Int4{}
		} else {
			return dst.Set(*value)
		}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Int4", value)
	}

	return nil
}

func (dst Int4) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.Int
}

func (src *Int4) AssignTo(dst interface{}) error {
	return int64AssignTo(int64(src.Int), src.Valid, dst)
}

func (dst *Int4) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Int4{}
		return nil
	}

	n, err := strconv.ParseInt(string(src), 10, 32)
	if err != nil {
		return err
	}

	*dst = Int4{Int: int32(n), Valid: true}
	return nil
}

func (dst *Int4) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Int4{}
		return nil
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length for int4: %v", len(src))
	}

	n := int32(binary.BigEndian.Uint32(src))
	*dst = Int4{Int: n, Valid: true}
	return nil
}

func (src Int4) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, strconv.FormatInt(int64(src.Int), 10)...), nil
}

func (src Int4) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return pgio.AppendInt32(buf, src.Int), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Int4) Scan(src interface{}) error {
	if src == nil {
		*dst = Int4{}
		return nil
	}

	switch src := src.(type) {
	case int64:
		if src < math.MinInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", src)
		}
		if src > math.MaxInt32 {
			return fmt.Errorf("%d is greater than maximum value for Int4", src)
		}
		*dst = Int4{Int: int32(src), Valid: true}
		return nil
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Int4) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return int64(src.Int), nil
}

func (src Int4) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}
	return []byte(strconv.FormatInt(int64(src.Int), 10)), nil
}

func (dst *Int4) UnmarshalJSON(b []byte) error {
	var n *int32
	err := json.Unmarshal(b, &n)
	if err != nil {
		return err
	}

	if n == nil {
		*dst = Int4{}
	} else {
		*dst = Int4{Int: *n, Valid: true}
	}

	return nil
}
