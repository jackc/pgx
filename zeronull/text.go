package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"
)

type Text string

func (dst *Text) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Text
	err := nullable.DecodeText(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Text(nullable.String)
	} else {
		*dst = Text("")
	}

	return nil
}

func (dst *Text) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	var nullable pgtype.Text
	err := nullable.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if nullable.Status == pgtype.Present {
		*dst = Text(nullable.String)
	} else {
		*dst = Text("")
	}

	return nil
}

func (src Text) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == Text("") {
		return nil, nil
	}

	nullable := pgtype.Text{
		String: string(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeText(ci, buf)
}

func (src Text) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if src == Text("") {
		return nil, nil
	}

	nullable := pgtype.Text{
		String: string(src),
		Status: pgtype.Present,
	}

	return nullable.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Text) Scan(src interface{}) error {
	if src == nil {
		*dst = Text("")
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
	return pgtype.EncodeValueText(src)
}
