package pgtype

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
)

type Bytea struct {
	Bytes  []byte
	Status Status
}

func (dst *Bytea) Set(src interface{}) error {
	if src == nil {
		*dst = Bytea{Status: Null}
		return nil
	}

	switch value := src.(type) {
	case []byte:
		if value != nil {
			*dst = Bytea{Bytes: value, Status: Present}
		} else {
			*dst = Bytea{Status: Null}
		}
	default:
		if originalSrc, ok := underlyingBytesType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Bytea", value)
	}

	return nil
}

func (dst *Bytea) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Bytes
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Bytea) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
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
		}
	case Null:
		return nullAssignTo(dst)
	}

	return fmt.Errorf("cannot decode %v into %T", src, dst)
}

// DecodeText only supports the hex format. This has been the default since
// PostgreSQL 9.0.
func (dst *Bytea) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Bytea{Status: Null}
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

	*dst = Bytea{Bytes: buf, Status: Present}
	return nil
}

func (dst *Bytea) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Bytea{Status: Null}
		return nil
	}

	buf := make([]byte, len(src))
	copy(buf, src)

	*dst = Bytea{Bytes: buf, Status: Present}
	return nil
}

func (src Bytea) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, `\x`)
	if err != nil {
		return false, err
	}

	_, err = io.WriteString(w, hex.EncodeToString(src.Bytes))
	return false, err
}

func (src Bytea) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := w.Write(src.Bytes)
	return false, err
}

// Scan implements the database/sql Scanner interface.
func (dst *Bytea) Scan(src interface{}) error {
	if src == nil {
		*dst = Bytea{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		buf := make([]byte, len(src))
		copy(buf, src)
		*dst = Bytea{Bytes: buf, Status: Present}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Bytea) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		return src.Bytes, nil
	case Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}
