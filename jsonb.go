package pgtype

import (
	"database/sql/driver"
	"fmt"
)

type Jsonb Json

func (dst *Jsonb) Set(src interface{}) error {
	return (*Json)(dst).Set(src)
}

func (dst *Jsonb) Get() interface{} {
	return (*Json)(dst).Get()
}

func (src *Jsonb) AssignTo(dst interface{}) error {
	return (*Json)(src).AssignTo(dst)
}

func (dst *Jsonb) DecodeText(ci *ConnInfo, src []byte) error {
	return (*Json)(dst).DecodeText(ci, src)
}

func (dst *Jsonb) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Jsonb{Status: Null}
		return nil
	}

	if len(src) == 0 {
		return fmt.Errorf("jsonb too short")
	}

	if src[0] != 1 {
		return fmt.Errorf("unknown jsonb version number %d", src[0])
	}

	*dst = Jsonb{Bytes: src[1:], Status: Present}
	return nil

}

func (src *Jsonb) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	return (*Json)(src).EncodeText(ci, buf)
}

func (src *Jsonb) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}

	buf = append(buf, 1)
	return append(buf, src.Bytes...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Jsonb) Scan(src interface{}) error {
	return (*Json)(dst).Scan(src)
}

// Value implements the database/sql/driver Valuer interface.
func (src *Jsonb) Value() (driver.Value, error) {
	return (*Json)(src).Value()
}
