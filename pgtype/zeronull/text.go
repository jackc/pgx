package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgx/v5/pgtype"
)

type Text string

func (Text) SkipUnderlyingTypePlan() {}

// ScanText implements the TextScanner interface.
func (dst *Text) ScanText(v pgtype.Text) error {
	if !v.Valid {
		*dst = ""
		return nil
	}

	*dst = Text(v.String)

	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Text) Scan(src any) error {
	if src == nil {
		*dst = ""
		return nil
	}

	var nullable pgtype.Text
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Text(nullable.String)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Text) Value() (driver.Value, error) {
	if src == "" {
		return nil, nil
	}
	return string(src), nil
}
