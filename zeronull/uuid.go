package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type UUID [16]byte

func (dst *UUID) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.UUID
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = UUID(nullable.Bytes)
	} else {
		*dst = UUID{}
	}

	return nil
}

func (dst *UUID) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.UUID
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = UUID(nullable.Bytes)
	} else {
		*dst = UUID{}
	}

	return nil
}

func (src UUID) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if (src == UUID{}) {
		return nil, nil
	}

	nullable := pgtype.UUID{
		Bytes:  [16]byte(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src UUID) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if (src == UUID{}) {
		return nil, nil
	}

	nullable := pgtype.UUID{
		Bytes:  [16]byte(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *UUID) Scan(src interface{}) error {
	if src == nil {
		*dst = UUID{}
		return nil
	}

	var nullable pgtype.UUID
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = UUID(nullable.Bytes)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src UUID) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
