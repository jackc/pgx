package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Float8 float64

func (dst *Float8) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Float8
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Float8(nullable.Float)
	} else {
		*dst = 0
	}

	return nil
}

func (dst *Float8) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Float8
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Float8(nullable.Float)
	} else {
		*dst = 0
	}

	return nil
}

func (src Float8) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Float8{
		Float:  float64(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Float8) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Float8{
		Float:  float64(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Float8) Scan(src interface{}) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Float8
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Float8(nullable.Float)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Float8) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
