package pgtype

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

type Varbit struct {
	Bytes  []byte
	Len    int32 // Number of bits
	Status Status
}

func (dst *Varbit) Set(src interface{}) error {
	return fmt.Errorf("cannot convert %v to Varbit", src)
}

func (dst *Varbit) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Varbit) AssignTo(dst interface{}) error {
	return fmt.Errorf("cannot assign %v to %T", src, dst)
}

func (dst *Varbit) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Varbit{Status: Null}
		return nil
	}

	*dst = Varbit{String: string(src), Status: Present}
	return nil
}

func (dst *Varbit) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Varbit{Status: Null}
		return nil
	}

	bitLen, err := pgio.ReadInt32(src)
	if err != nil {
		return err
	}
	rp := 4

	buf := make([]byte, len(src[rp:]))
	copy(buf, src[rp:])

	*dst = Varbit{Bytes: buf, Len: bitLen, Status: Present}
	return nil
}

func (src *Varbit) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, src.String)
	return false, err
}

func (src *Varbit) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	if _, err := pgio.WriteInt32(src.Len); err != nil {
		return false, err
	}

	_, err := w.Write(src.Bytes)
	return false, err
}

// Scan implements the database/sql Scanner interface.
func (dst *Varbit) Scan(src interface{}) error {
	if src == nil {
		*dst = Varbit{Status: Null}
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
func (src *Varbit) Value() (driver.Value, error) {
	return encodeValueText(src)
}
