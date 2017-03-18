package pgtype

import (
	"io"
)

// GenericText is a placeholder for text format values that no other type exists
// to handle.
type GenericText Text

func (dst *GenericText) Set(src interface{}) error {
	return (*Text)(dst).Set(src)
}

func (dst *GenericText) Get() interface{} {
	return (*Text)(dst).Get()
}

func (src *GenericText) AssignTo(dst interface{}) error {
	return (*Text)(src).AssignTo(dst)
}

func (dst *GenericText) DecodeText(ci *ConnInfo, src []byte) error {
	return (*Text)(dst).DecodeText(ci, src)
}

func (src GenericText) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	return (Text)(src).EncodeText(ci, w)
}
