package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type Text struct {
	String string
	Valid  bool
}

func (dst *Text) Set(src interface{}) error {
	if src == nil {
		*dst = Text{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case string:
		*dst = Text{String: value, Valid: true}
	case *string:
		if value == nil {
			*dst = Text{}
		} else {
			*dst = Text{String: *value, Valid: true}
		}
	case []byte:
		if value == nil {
			*dst = Text{}
		} else {
			*dst = Text{String: string(value), Valid: true}
		}
	case fmt.Stringer:
		if value == fmt.Stringer(nil) {
			*dst = Text{}
		} else {
			*dst = Text{String: value.String(), Valid: true}
		}
	default:
		// Cannot be part of the switch: If Value() returns nil on
		// non-string, we should still try to checks the underlying type
		// using reflection.
		//
		// For example the struct might implement driver.Valuer with
		// pointer receiver and fmt.Stringer with value receiver.
		if value, ok := src.(driver.Valuer); ok {
			if value == driver.Valuer(nil) {
				*dst = Text{}
				return nil
			} else {
				v, err := value.Value()
				if err != nil {
					return fmt.Errorf("driver.Valuer Value() method failed: %w", err)
				}

				// Handles also v == nil case.
				if s, ok := v.(string); ok {
					*dst = Text{String: s, Valid: true}
					return nil
				}
			}
		}

		if originalSrc, ok := underlyingStringType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Text", value)
	}

	return nil
}

func (dst Text) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.String
}

func (src *Text) AssignTo(dst interface{}) error {
	if !src.Valid {
		return NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *string:
		*v = src.String
		return nil
	case *[]byte:
		*v = make([]byte, len(src.String))
		copy(*v, src.String)
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}
}

func (Text) PreferredResultFormat() int16 {
	return TextFormatCode
}

func (dst *Text) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Text{}
		return nil
	}

	*dst = Text{String: string(src), Valid: true}
	return nil
}

func (dst *Text) DecodeBinary(ci *ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (Text) PreferredParamFormat() int16 {
	return TextFormatCode
}

func (src Text) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, src.String...), nil
}

func (src Text) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	return src.EncodeText(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Text) Scan(src interface{}) error {
	if src == nil {
		*dst = Text{}
		return nil
	}

	switch src := src.(type) {
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
func (src Text) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return src.String, nil
}

func (src Text) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(src.String)
}

func (dst *Text) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*dst = Text{}
	} else {
		*dst = Text{String: *s, Valid: true}
	}

	return nil
}
