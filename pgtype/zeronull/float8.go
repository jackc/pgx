package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgx/v5/pgtype"
)

type Float8 float64

func (Float8) SkipUnderlyingTypePlan() {}

// ScanFloat64 implements the Float64Scanner interface.
func (f *Float8) ScanFloat64(n pgtype.Float8) error {
	if !n.Valid {
		*f = 0
		return nil
	}

	*f = Float8(n.Float64)

	return nil
}

func (f Float8) Float64Value() (pgtype.Float8, error) {
	if f == 0 {
		return pgtype.Float8{}, nil
	}
	return pgtype.Float8{Float64: float64(f), Valid: true}, nil
}

// Scan implements the database/sql Scanner interface.
func (f *Float8) Scan(src any) error {
	if src == nil {
		*f = 0
		return nil
	}

	var nullable pgtype.Float8
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*f = Float8(nullable.Float64)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (f Float8) Value() (driver.Value, error) {
	if f == 0 {
		return nil, nil
	}
	return float64(f), nil
}
