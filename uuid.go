package pgtype

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
)

type Uuid struct {
	Bytes  [16]byte
	Status Status
}

func (dst *Uuid) Set(src interface{}) error {
	switch value := src.(type) {
	case [16]byte:
		*dst = Uuid{Bytes: value, Status: Present}
	case []byte:
		if len(value) != 16 {
			return fmt.Errorf("[]byte must be 16 bytes to convert to Uuid: %d", len(value))
		}
		*dst = Uuid{Status: Present}
		copy(dst.Bytes[:], value)
	case string:
		uuid, err := parseUuid(value)
		if err != nil {
			return err
		}
		*dst = Uuid{Bytes: uuid, Status: Present}
	default:
		if originalSrc, ok := underlyingPtrType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Uuid", value)
	}

	return nil
}

func (dst *Uuid) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Bytes
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Uuid) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *[16]byte:
			*v = src.Bytes
			return nil
		case *[]byte:
			*v = make([]byte, 16)
			copy(*v, src.Bytes[:])
			return nil
		case *string:
			*v = encodeUuid(src.Bytes)
			return nil
		default:
			if nextDst, retry := GetAssignToDstType(v); retry {
				return src.AssignTo(nextDst)
			}
		}
	case Null:
		return NullAssignTo(dst)
	}

	return fmt.Errorf("cannot assign %v into %T", src, dst)
}

// parseUuid converts a string UUID in standard form to a byte array.
func parseUuid(src string) (dst [16]byte, err error) {
	src = src[0:8] + src[9:13] + src[14:18] + src[19:23] + src[24:]
	buf, err := hex.DecodeString(src)
	if err != nil {
		return dst, err
	}

	copy(dst[:], buf)
	return dst, err
}

// encodeUuid converts a uuid byte array to UUID standard string form.
func encodeUuid(src [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", src[0:4], src[4:6], src[6:8], src[8:10], src[10:16])
}

func (dst *Uuid) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Uuid{Status: Null}
		return nil
	}

	if len(src) != 36 {
		return fmt.Errorf("invalid length for Uuid: %v", len(src))
	}

	buf, err := parseUuid(string(src))
	if err != nil {
		return err
	}

	*dst = Uuid{Bytes: buf, Status: Present}
	return nil
}

func (dst *Uuid) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Uuid{Status: Null}
		return nil
	}

	if len(src) != 16 {
		return fmt.Errorf("invalid length for Uuid: %v", len(src))
	}

	*dst = Uuid{Status: Present}
	copy(dst.Bytes[:], src)
	return nil
}

func (src Uuid) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, encodeUuid(src.Bytes))
	return false, err
}

func (src Uuid) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := w.Write(src.Bytes[:])
	return false, err
}

// Scan implements the database/sql Scanner interface.
func (dst *Uuid) Scan(src interface{}) error {
	if src == nil {
		*dst = Uuid{Status: Null}
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
func (src Uuid) Value() (driver.Value, error) {
	return encodeValueText(src)
}
