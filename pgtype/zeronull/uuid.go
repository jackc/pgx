package zeronull

import (
	"database/sql/driver"

	"github.com/jackc/pgx/v5/pgtype"
)

type UUID [16]byte

func (UUID) SkipUnderlyingTypePlan() {}

// ScanUUID implements the UUIDScanner interface.
func (u *UUID) ScanUUID(v pgtype.UUID) error {
	if !v.Valid {
		*u = UUID{}
		return nil
	}

	*u = UUID(v.Bytes)

	return nil
}

func (u UUID) UUIDValue() (pgtype.UUID, error) {
	if u == (UUID{}) {
		return pgtype.UUID{}, nil
	}
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

// Scan implements the database/sql Scanner interface.
func (u *UUID) Scan(src any) error {
	if src == nil {
		*u = UUID{}
		return nil
	}

	var nullable pgtype.UUID
	err := nullable.Scan(src)
	if err != nil {
		return err
	}

	*u = UUID(nullable.Bytes)

	return nil
}

// Value implements the database/sql/driver Valuer interface.
func (u UUID) Value() (driver.Value, error) {
	if u == (UUID{}) {
		return nil, nil
	}

	buf, err := pgtype.UUIDCodec{}.PlanEncode(nil, pgtype.UUIDOID, pgtype.TextFormatCode, u).Encode(u, nil)
	if err != nil {
		return nil, err
	}

	return string(buf), nil
}
