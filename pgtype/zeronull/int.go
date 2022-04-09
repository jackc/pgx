// Do not edit. Generated from pgtype/zeronull/int.go.erb
package zeronull

import (
	"database/sql/driver"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5/pgtype"
)

type Int2 int16

func (Int2) SkipUnderlyingTypePlan() {}

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
func (dst *Int2) Scan(src any) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Int2
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Int2(nullable.Int16)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int2) Value() (driver.Value, error) {
	if src == 0 {
		return nil, nil
	}
	return int64(src), nil
}

type Int4 int32

func (Int4) SkipUnderlyingTypePlan() {}

// ScanInt64 implements the Int64Scanner interface.
func (dst *Int4) ScanInt64(n int64, valid bool) error {
	if !valid {
		*dst = 0
		return nil
	}

	if n < math.MinInt32 {
		return fmt.Errorf("%d is greater than maximum value for Int4", n)
	}
	if n > math.MaxInt32 {
		return fmt.Errorf("%d is greater than maximum value for Int4", n)
	}
	*dst = Int4(n)

	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Int4) Scan(src any) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Int4
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Int4(nullable.Int32)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int4) Value() (driver.Value, error) {
	if src == 0 {
		return nil, nil
	}
	return int64(src), nil
}

type Int8 int64

func (Int8) SkipUnderlyingTypePlan() {}

// ScanInt64 implements the Int64Scanner interface.
func (dst *Int8) ScanInt64(n int64, valid bool) error {
	if !valid {
		*dst = 0
		return nil
	}

	if n < math.MinInt64 {
		return fmt.Errorf("%d is greater than maximum value for Int8", n)
	}
	if n > math.MaxInt64 {
		return fmt.Errorf("%d is greater than maximum value for Int8", n)
	}
	*dst = Int8(n)

	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Int8) Scan(src any) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Int8
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Int8(nullable.Int64)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int8) Value() (driver.Value, error) {
	if src == 0 {
		return nil, nil
	}
	return int64(src), nil
}
