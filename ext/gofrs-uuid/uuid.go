package uuid

import (
	"database/sql/driver"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgtype"
)

type UUID struct {
	UUID  uuid.UUID
	Valid bool
}

func (dst *UUID) Set(src interface{}) error {
	if src == nil {
		*dst = UUID{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case uuid.UUID:
		*dst = UUID{UUID: value, Valid: true}
	case [16]byte:
		*dst = UUID{UUID: uuid.UUID(value), Valid: true}
	case []byte:
		if len(value) != 16 {
			return fmt.Errorf("[]byte must be 16 bytes to convert to UUID: %d", len(value))
		}
		*dst = UUID{Valid: true}
		copy(dst.UUID[:], value)
	case string:
		uuid, err := uuid.FromString(value)
		if err != nil {
			return err
		}
		*dst = UUID{UUID: uuid, Valid: true}
	default:
		// If all else fails see if pgtype.UUID can handle it. If so, translate through that.
		pgUUID := &pgtype.UUID{}
		if err := pgUUID.Set(value); err != nil {
			return fmt.Errorf("cannot convert %v to UUID", value)
		}

		*dst = UUID{UUID: uuid.UUID(pgUUID.Bytes), Valid: pgUUID.Valid}
	}

	return nil
}

func (dst UUID) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.UUID
}

func (src *UUID) AssignTo(dst interface{}) error {
	if !src.Valid {
		return pgtype.NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *uuid.UUID:
		*v = src.UUID
		return nil
	case *[16]byte:
		*v = [16]byte(src.UUID)
		return nil
	case *[]byte:
		*v = make([]byte, 16)
		copy(*v, src.UUID[:])
		return nil
	case *string:
		*v = src.UUID.String()
		return nil
	default:
		if nextDst, retry := pgtype.GetAssignToDstType(v); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}
}

func (dst *UUID) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = UUID{}
		return nil
	}

	u, err := uuid.FromString(string(src))
	if err != nil {
		return err
	}

	*dst = UUID{UUID: u, Valid: true}
	return nil
}

func (dst *UUID) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = UUID{}
		return nil
	}

	if len(src) != 16 {
		return fmt.Errorf("invalid length for UUID: %v", len(src))
	}

	*dst = UUID{Valid: true}
	copy(dst.UUID[:], src)
	return nil
}

func (src UUID) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}
	return append(buf, src.UUID.String()...), nil
}

func (src UUID) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}
	return append(buf, src.UUID[:]...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *UUID) Scan(src interface{}) error {
	if src == nil {
		*dst = UUID{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src UUID) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}

func (src UUID) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}
	return []byte(`"` + src.UUID.String() + `"`), nil
}

func (dst *UUID) UnmarshalJSON(b []byte) error {
	u := uuid.NullUUID{}
	err := u.UnmarshalJSON(b)
	if err != nil {
		return err
	}

	*dst = UUID{UUID: u.UUID, Valid: u.Valid}

	return nil
}
