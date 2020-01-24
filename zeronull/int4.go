package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Int4 int32

func (dst *Int4) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Int4
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Int4(nullable.Int)
	} else {
		*dst = 0
	}

	return nil
}

func (dst *Int4) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Int4
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Int4(nullable.Int)
	} else {
		*dst = 0
	}

	return nil
}

func (src Int4) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Int4{
		Int:    int32(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Int4) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Int4{
		Int:    int32(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Int4) Scan(src interface{}) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Int4
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Int4(nullable.Int)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int4) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
