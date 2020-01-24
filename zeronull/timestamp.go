package zeronull

import (
	"database/sql/driver"
	"time"

	"github.com/jackc/pgtype"
)

type Timestamp time.Time

func (dst *Timestamp) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Timestamp
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Timestamp(nullable.Time)
	} else {
		*dst = Timestamp{}
	}

	return nil
}

func (dst *Timestamp) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Timestamp
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Timestamp(nullable.Time)
	} else {
		*dst = Timestamp{}
	}

	return nil
}

func (src Timestamp) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if (src == Timestamp{}) {
		return nil, nil
	}

	nullable := pgtype.Timestamp{
		Time:   time.Time(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Timestamp) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if (src == Timestamp{}) {
		return nil, nil
	}

	nullable := pgtype.Timestamp{
		Time:   time.Time(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Timestamp) Scan(src interface{}) error {
	if src == nil {
		*dst = Timestamp{}
		return nil
	}

	var nullable pgtype.Timestamp
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Timestamp(nullable.Time)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Timestamp) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
