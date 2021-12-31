package zeronull

import (
	"database/sql/driver"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5/pgtype"
)

type Int2 int16

// ScanInt64 implements the Int64Scanner interface.
func (dst *Int2) ScanInt64(n int64, valid bool) error {
	if !valid {
		*dst = 0
		return nil
	}

	if n < math.MinInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	if n > math.MaxInt16 {
		return fmt.Errorf("%d is greater than maximum value for Int2", n)
	}
	*dst = Int2(n)

	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Int2) Scan(src interface{}) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Int2
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Int2(nullable.Int)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int2) Value() (driver.Value, error) {
	if src == 0 {
		return nil, nil
	}
	return int64(src), nil
}
