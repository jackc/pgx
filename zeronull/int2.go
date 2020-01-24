package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Int2 int16

func (dst *Int2) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Int2
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Int2(nullable.Int)
	} else {
		*dst = 0
	}

	return nil
}

func (dst *Int2) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Int2
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Int2(nullable.Int)
	} else {
		*dst = 0
	}

	return nil
}

func (src Int2) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Int2{
		Int:    int16(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Int2) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Int2{
		Int:    int16(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
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
	return pgtype.EncodeValueText(src)
}
