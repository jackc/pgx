package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
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

	bitLen := len(src)
	byteLen := bitLen / 8
	if bitLen%8 > 0 {
		byteLen++
	}
	buf := make([]byte, byteLen)

	for i, b := range src {
		if b == '1' {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			buf[byteIdx] = buf[byteIdx] | (128 >> bitIdx)
		}
	}

	*dst = Varbit{Bytes: buf, Len: int32(bitLen), Status: Present}
	return nil
}

func (dst *Varbit) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Varbit{Status: Null}
		return nil
	}

	if len(src) < 4 {
		return fmt.Errorf("invalid length for varbit: %v", len(src))
	}

	bitLen := int32(binary.BigEndian.Uint32(src))
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

	buf := make([]byte, int(src.Len))
	for i, _ := range buf {
		byteIdx := i / 8
		bitMask := byte(128 >> byte(i%8))
		char := byte('0')
		if src.Bytes[byteIdx]&bitMask > 0 {
			char = '1'
		}
		buf[i] = char
	}

	_, err := w.Write(buf)
	return false, err
}

func (src *Varbit) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	if _, err := pgio.WriteInt32(w, src.Len); err != nil {
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
