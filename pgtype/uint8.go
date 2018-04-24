package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"

	"github.com/jackc/pgx/pgio"
	"github.com/pkg/errors"
)

type Uint8 struct {
	Uint   uint64
	Status Status
}

func (dst *Uint8) Set(src interface{}) error {
	if src == nil {
		*dst = Uint8{Status: Null}
		return nil
	}

	switch value := src.(type) {
	case int8:
		if value < 0 {
			return errors.Errorf("%d is less than zero for Uint8", value)
		}
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case uint8:
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case int16:
		if value < 0 {
			return errors.Errorf("%d is less than zero for Uint8", value)
		}
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case uint16:
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case int32:
		if value < 0 {
			return errors.Errorf("%d is less than zero for Uint8", value)
		}
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case uint32:
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case int64:
		if value < 0 {
			return errors.Errorf("%d is less than zero for Uint8", value)
		}
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case uint64:
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case int:
		if value < 0 {
			return errors.Errorf("%d is less than zero for Uint8", value)
		}
		if uint64(value) > math.MaxUint64 {
			return errors.Errorf("%d is greater than maximum value for Uint8", value)
		}
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case uint:
		if uint64(value) > math.MaxUint64 {
			return errors.Errorf("%d is greater than maximum value for Uint8", value)
		}
		*dst = Uint8{Uint: uint64(value), Status: Present}
	case string:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		*dst = Uint8{Uint: num, Status: Present}
	default:
		if originalSrc, ok := underlyingNumberType(src); ok {
			return dst.Set(originalSrc)
		}
		return errors.Errorf("cannot convert %v to Uint8", value)
	}

	return nil
}

func (dst *Uint8) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Uint
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Uint8) AssignTo(dst interface{}) error {
	return uint64AssignTo(src.Uint, src.Status, dst)
}

func (dst *Uint8) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Uint8{Status: Null}
		return nil
	}

	n, err := strconv.ParseUint(string(src), 10, 64)
	if err != nil {
		return err
	}

	*dst = Uint8{Uint: n, Status: Present}
	return nil
}

func (dst *Uint8) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Uint8{Status: Null}
		return nil
	}

	if len(src) != 8 {
		return errors.Errorf("invalid length for uint8: %v", len(src))
	}

	n := binary.BigEndian.Uint64(src)

	*dst = Uint8{Uint: n, Status: Present}
	return nil
}

func (src *Uint8) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}

	return append(buf, strconv.FormatUint(src.Uint, 10)...), nil
}

func (src *Uint8) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}

	return pgio.AppendUint64(buf, src.Uint), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Uint8) Scan(src interface{}) error {
	if src == nil {
		*dst = Uint8{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case uint64:
		*dst = Uint8{Uint: src, Status: Present}
		return nil
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src *Uint8) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		return src.Uint, nil
	case Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}

func (src *Uint8) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case Present:
		return []byte(strconv.FormatUint(src.Uint, 10)), nil
	case Null:
		return []byte("null"), nil
	case Undefined:
		return nil, errUndefined
	}

	return nil, errBadStatus
}

func (dst *Uint8) UnmarshalJSON(b []byte) error {
	var n uint64
	err := json.Unmarshal(b, &n)
	if err != nil {
		return err
	}

	*dst = Uint8{Uint: n, Status: Present}

	return nil
}
