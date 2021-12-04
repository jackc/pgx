package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/jackc/pgio"
)

// pguint32 is the core type that is used to implement PostgreSQL types such as
// CID and XID.
type pguint32 struct {
	Uint  uint32
	Valid bool
}

// Set converts from src to dst. Note that as pguint32 is not a general
// number type Set does not do automatic type conversion as other number
// types do.
func (dst *pguint32) Set(src interface{}) error {
	switch value := src.(type) {
	case int64:
		if value < 0 {
			return fmt.Errorf("%d is less than minimum value for pguint32", value)
		}
		if value > math.MaxUint32 {
			return fmt.Errorf("%d is greater than maximum value for pguint32", value)
		}
		*dst = pguint32{Uint: uint32(value), Valid: true}
	case uint32:
		*dst = pguint32{Uint: value, Valid: true}
	default:
		return fmt.Errorf("cannot convert %v to pguint32", value)
	}

	return nil
}

func (dst pguint32) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.Uint
}

// AssignTo assigns from src to dst. Note that as pguint32 is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *pguint32) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *uint32:
		if src.Valid {
			*v = src.Uint
		} else {
			return fmt.Errorf("cannot assign %v into %T", src, dst)
		}
	case **uint32:
		if src.Valid {
			n := src.Uint
			*v = &n
		} else {
			*v = nil
		}
	}

	return nil
}

func (dst *pguint32) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = pguint32{}
		return nil
	}

	n, err := strconv.ParseUint(string(src), 10, 32)
	if err != nil {
		return err
	}

	*dst = pguint32{Uint: uint32(n), Valid: true}
	return nil
}

func (dst *pguint32) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = pguint32{}
		return nil
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length: %v", len(src))
	}

	n := binary.BigEndian.Uint32(src)
	*dst = pguint32{Uint: n, Valid: true}
	return nil
}

func (src pguint32) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, strconv.FormatUint(uint64(src.Uint), 10)...), nil
}

func (src pguint32) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return pgio.AppendUint32(buf, src.Uint), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *pguint32) Scan(src interface{}) error {
	if src == nil {
		*dst = pguint32{}
		return nil
	}

	switch src := src.(type) {
	case uint32:
		*dst = pguint32{Uint: src, Valid: true}
		return nil
	case int64:
		*dst = pguint32{Uint: uint32(src), Valid: true}
		return nil
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src pguint32) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return int64(src.Uint), nil
}
