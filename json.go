package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

type JSON struct {
	Bytes []byte
	Valid bool
}

func (dst *JSON) Set(src interface{}) error {
	if src == nil {
		*dst = JSON{}
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
		*dst = JSON{Bytes: []byte(value), Valid: true}
	case *string:
		if value == nil {
			*dst = JSON{}
		} else {
			*dst = JSON{Bytes: []byte(*value), Valid: true}
		}
	case []byte:
		if value == nil {
			*dst = JSON{}
		} else {
			*dst = JSON{Bytes: value, Valid: true}
		}
	// Encode* methods are defined on *JSON. If JSON is passed directly then the
	// struct itself would be encoded instead of Bytes. This is clearly a footgun
	// so detect and return an error. See https://github.com/jackc/pgx/issues/350.
	case JSON:
		return errors.New("use pointer to pgtype.JSON instead of value")
	// Same as above but for JSONB (because they share implementation)
	case JSONB:
		return errors.New("use pointer to pgtype.JSONB instead of value")

	default:
		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}
		*dst = JSON{Bytes: buf, Valid: true}
	}

	return nil
}

func (dst JSON) Get() interface{} {
	if !dst.Valid {
		return nil
	}

	var i interface{}
	err := json.Unmarshal(dst.Bytes, &i)
	if err != nil {
		return dst
	}
	return i
}

func (src *JSON) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *string:
		if src.Valid {
			*v = string(src.Bytes)
		} else {
			return fmt.Errorf("cannot assign non-valid to %T", dst)
		}
	case **string:
		if src.Valid {
			s := string(src.Bytes)
			*v = &s
			return nil
		} else {
			*v = nil
			return nil
		}
	case *[]byte:
		if !src.Valid {
			*v = nil
		} else {
			buf := make([]byte, len(src.Bytes))
			copy(buf, src.Bytes)
			*v = buf
		}
	default:
		data := src.Bytes
		if data == nil || !src.Valid {
			data = []byte("null")
		}

		return json.Unmarshal(data, dst)
	}

	return nil
}

func (JSON) PreferredResultFormat() int16 {
	return TextFormatCode
}

func (dst *JSON) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = JSON{}
		return nil
	}

	*dst = JSON{Bytes: src, Valid: true}
	return nil
}

func (dst *JSON) DecodeBinary(ci *ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (JSON) PreferredParamFormat() int16 {
	return TextFormatCode
}

func (src JSON) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, src.Bytes...), nil
}

func (src JSON) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	return src.EncodeText(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *JSON) Scan(src interface{}) error {
	if src == nil {
		*dst = JSON{}
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
func (src JSON) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return src.Bytes, nil
}

func (src JSON) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}
	return src.Bytes, nil
}

func (dst *JSON) UnmarshalJSON(b []byte) error {
	if b == nil || string(b) == "null" {
		*dst = JSON{}
	} else {
		*dst = JSON{Bytes: b, Valid: true}
	}
	return nil

}
