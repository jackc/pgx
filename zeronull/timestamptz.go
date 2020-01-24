package zeronull

import (
	"database/sql/driver"
	"time"

	"github.com/jackc/pgtype"
)

type Timestamptz time.Time

func (dst *Timestamptz) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Timestamptz
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Timestamptz(nullable.Time)
	} else {
		*dst = Timestamptz{}
	}

	return nil
}

func (dst *Timestamptz) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Timestamptz
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Timestamptz(nullable.Time)
	} else {
		*dst = Timestamptz{}
	}

	return nil
}

func (src Timestamptz) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if (src == Timestamptz{}) {
		return nil, nil
	}

	nullable := pgtype.Timestamptz{
		Time:   time.Time(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Timestamptz) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if (src == Timestamptz{}) {
		return nil, nil
	}

	nullable := pgtype.Timestamptz{
		Time:   time.Time(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Timestamptz) Scan(src interface{}) error {
	if src == nil {
		*dst = Timestamptz{}
		return nil
	}

	var nullable pgtype.Timestamptz
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*dst = Timestamptz(nullable.Time)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (src Timestamptz) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
