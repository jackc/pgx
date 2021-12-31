package pgtype

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
)

type Int2 struct {
	Int   int16
	Valid bool
}

// Scan implements the database/sql Scanner interface.
func (dst *Int2) Scan(src interface{}) error {
	if src == nil {
		*dst = Int2{}
		return nil
	}

	var n int64

	switch src := src.(type) {
	case int64:
		n = src
	case string:
		var err error
		n, err = strconv.ParseInt(src, 10, 16)
		if err != nil {
			return err
		}
	case []byte:
		var err error
		n, err = strconv.ParseInt(string(src), 10, 16)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot scan %T", src)
	}

	if n < math.MinInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	if n > math.MaxInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	*dst = Int2{Int: int16(n), Valid: true}

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int2) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return int64(src.Int), nil
}

func (src Int2) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}
	return []byte(strconv.FormatInt(int64(src.Int), 10)), nil
}
