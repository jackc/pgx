package pgtype

import (
	"encoding/json"
	"io"
)

type Json struct {
	Bytes  []byte
	Status Status
}

func (dst *Json) Set(src interface{}) error {
	switch value := src.(type) {
	case string:
		*dst = Json{Bytes: []byte(value), Status: Present}
	case *string:
		if value == nil {
			*dst = Json{Status: Null}
		} else {
			*dst = Json{Bytes: []byte(*value), Status: Present}
		}
	case []byte:
		if value == nil {
			*dst = Json{Status: Null}
		} else {
			*dst = Json{Bytes: value, Status: Present}
		}
	default:
		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}
		*dst = Json{Bytes: buf, Status: Present}
	}

	return nil
}

func (dst *Json) Get() interface{} {
	switch dst.Status {
	case Present:
		var i interface{}
		err := json.Unmarshal(dst.Bytes, &i)
		if err != nil {
			return dst
		}
		return i
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Json) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *string:
		if src.Status != Present {
			v = nil
		} else {
			*v = string(src.Bytes)
		}
	case **string:
		*v = new(string)
		return src.AssignTo(*v)
	case *[]byte:
		if src.Status != Present {
			*v = nil
		} else {
			buf := make([]byte, len(src.Bytes))
			copy(buf, src.Bytes)
			*v = buf
		}
	default:
		data := src.Bytes
		if data == nil || src.Status != Present {
			data = []byte("null")
		}

		return json.Unmarshal(data, dst)
	}

	return nil
}

func (dst *Json) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Json{Status: Null}
		return nil
	}

	buf := make([]byte, len(src))
	copy(buf, src)

	*dst = Json{Bytes: buf, Status: Present}
	return nil
}

func (dst *Json) DecodeBinary(ci *ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (src Json) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := w.Write(src.Bytes)
	return false, err
}

func (src Json) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	return src.EncodeText(ci, w)
}
