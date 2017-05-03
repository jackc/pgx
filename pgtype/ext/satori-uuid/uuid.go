package uuid

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/jackc/pgx/pgtype"
	uuid "github.com/satori/go.uuid"
)

var errUndefined = errors.New("cannot encode status undefined")

type Uuid struct {
	UUID   uuid.UUID
	Status pgtype.Status
}

func (dst *Uuid) Set(src interface{}) error {
	switch value := src.(type) {
	case uuid.UUID:
		*dst = Uuid{UUID: value, Status: pgtype.Present}
	case [16]byte:
		*dst = Uuid{UUID: uuid.UUID(value), Status: pgtype.Present}
	case []byte:
		if len(value) != 16 {
			return fmt.Errorf("[]byte must be 16 bytes to convert to Uuid: %d", len(value))
		}
		*dst = Uuid{Status: pgtype.Present}
		copy(dst.UUID[:], value)
	case string:
		uuid, err := uuid.FromString(value)
		if err != nil {
			return err
		}
		*dst = Uuid{UUID: uuid, Status: pgtype.Present}
	default:
		// If all else fails see if pgtype.Uuid can handle it. If so, translate through that.
		pgUuid := &pgtype.Uuid{}
		if err := pgUuid.Set(value); err != nil {
			return fmt.Errorf("cannot convert %v to Uuid", value)
		}

		*dst = Uuid{UUID: uuid.UUID(pgUuid.Bytes), Status: pgUuid.Status}
	}

	return nil
}

func (dst *Uuid) Get() interface{} {
	switch dst.Status {
	case pgtype.Present:
		return dst.UUID
	case pgtype.Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Uuid) AssignTo(dst interface{}) error {
	switch src.Status {
	case pgtype.Present:
		switch v := dst.(type) {
		case *uuid.UUID:
			*v = src.UUID
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
		}
	case pgtype.Null:
		return pgtype.NullAssignTo(dst)
	}

	return fmt.Errorf("cannot assign %v into %T", src, dst)
}

func (dst *Uuid) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Uuid{Status: pgtype.Null}
		return nil
	}

	u, err := uuid.FromString(string(src))
	if err != nil {
		return err
	}

	*dst = Uuid{UUID: u, Status: pgtype.Present}
	return nil
}

func (dst *Uuid) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Uuid{Status: pgtype.Null}
		return nil
	}

	if len(src) != 16 {
		return fmt.Errorf("invalid length for Uuid: %v", len(src))
	}

	*dst = Uuid{Status: pgtype.Present}
	copy(dst.UUID[:], src)
	return nil
}

func (src *Uuid) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	return append(buf, src.UUID.String()...), nil
}

func (src *Uuid) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	return append(buf, src.UUID[:]...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Uuid) Scan(src interface{}) error {
	if src == nil {
		*dst = Uuid{Status: pgtype.Null}
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
func (src *Uuid) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}
