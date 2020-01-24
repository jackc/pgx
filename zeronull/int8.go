package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Int8 int64

func (dst *Int8) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Int8
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Int8(nullable.Int)
	} else {
		*dst = 0
	}

	return nil
}

func (dst *Int8) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Int8
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Int8(nullable.Int)
	} else {
		*dst = 0
	}

	return nil
}

func (src Int8) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Int8{
		Int:    int64(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Int8) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == 0 {
		return nil, nil
	}

	nullable := pgtype.Int8{
		Int:    int64(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Int8) Scan(src interface{}) error {
	if src == nil {
		*dst = 0
		return nil
	}

	var nullable pgtype.Int8
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Int8(nullable.Int)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Int8) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
