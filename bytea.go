package pgtype

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
)

type Bytea struct {
	Bytes []byte
	Valid bool
}

func (dst *Bytea) Set(src interface{}) error {
	if src == nil {
		*dst = Bytea{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case []byte:
		if value != nil {
			*dst = Bytea{Bytes: value, Valid: true}
		} else {
			*dst = Bytea{}
		}
	default:
		if originalSrc, ok := underlyingBytesType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Bytea", value)
	}

	return nil
}

func (dst Bytea) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.Bytes
}

func (src *Bytea) AssignTo(dst interface{}) error {
	if !src.Valid {
		return NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *[]byte:
		buf := make([]byte, len(src.Bytes))
		copy(buf, src.Bytes)
		*v = buf
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}
}

// DecodeText only supports the hex format. This has been the default since
// PostgreSQL 9.0.
func (dst *Bytea) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Bytea{}
		return nil
	}

	if len(src) < 2 || src[0] != '\\' || src[1] != 'x' {
		return fmt.Errorf("invalid hex format")
	}

	buf := make([]byte, (len(src)-2)/2)
	_, err := hex.Decode(buf, src[2:])
	if err != nil {
		return err
	}

	*dst = Bytea{Bytes: buf, Valid: true}
	return nil
}

func (dst *Bytea) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Bytea{}
		return nil
	}

	*dst = Bytea{Bytes: src, Valid: true}
	return nil
}

func (src Bytea) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	buf = append(buf, `\x`...)
	buf = append(buf, hex.EncodeToString(src.Bytes)...)
	return buf, nil
}

func (src Bytea) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, src.Bytes...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Bytea) Scan(src interface{}) error {
	if src == nil {
		*dst = Bytea{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		buf := make([]byte, len(src))
		copy(buf, src)
		*dst = Bytea{Bytes: buf, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Bytea) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return src.Bytes, nil
}
