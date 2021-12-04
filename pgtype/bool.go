package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
)

type Bool struct {
	Bool  bool
	Valid bool
}

func (dst *Bool) Set(src interface{}) error {
	if src == nil {
		*dst = Bool{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case bool:
		*dst = Bool{Bool: value, Valid: true}
	case string:
		bb, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		*dst = Bool{Bool: bb, Valid: true}
	case *bool:
		if value == nil {
			*dst = Bool{}
		} else {
			return dst.Set(*value)
		}
	case *string:
		if value == nil {
			*dst = Bool{}
		} else {
			return dst.Set(*value)
		}
	default:
		if originalSrc, ok := underlyingBoolType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Bool", value)
	}

	return nil
}

func (dst Bool) Get() interface{} {
	if !dst.Valid {
		return nil
	}

	return dst.Bool
}

func (src *Bool) AssignTo(dst interface{}) error {
	if !src.Valid {
		return NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *bool:
		*v = src.Bool
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}
}

func (dst *Bool) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Bool{}
		return nil
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	*dst = Bool{Bool: src[0] == 't', Valid: true}
	return nil
}

func (dst *Bool) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Bool{}
		return nil
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	*dst = Bool{Bool: src[0] == 1, Valid: true}
	return nil
}

func (src Bool) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	if src.Bool {
		buf = append(buf, 't')
	} else {
		buf = append(buf, 'f')
	}

	return buf, nil
}

func (src Bool) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	if src.Bool {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	return buf, nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Bool) Scan(src interface{}) error {
	if src == nil {
		*dst = Bool{}
		return nil
	}

	switch src := src.(type) {
	case bool:
		*dst = Bool{Bool: src, Valid: true}
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
func (src Bool) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	return src.Bool, nil
}

func (src Bool) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	if src.Bool {
		return []byte("true"), nil
	} else {
		return []byte("false"), nil
	}
}

func (dst *Bool) UnmarshalJSON(b []byte) error {
	var v *bool
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	if v == nil {
		*dst = Bool{}
	} else {
		*dst = Bool{Bool: *v, Valid: true}
	}

	return nil
}
