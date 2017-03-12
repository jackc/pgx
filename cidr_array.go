package pgtype

import (
	"io"
)

type CidrArray InetArray

func (dst *CidrArray) Set(src interface{}) error {
	return (*InetArray)(dst).Set(src)
}

func (dst *CidrArray) Get() interface{} {
	return (*InetArray)(dst).Get()
}

func (src *CidrArray) AssignTo(dst interface{}) error {
	return (*InetArray)(src).AssignTo(dst)
}

func (dst *CidrArray) DecodeText(src []byte) error {
	return (*InetArray)(dst).DecodeText(src)
}

func (dst *CidrArray) DecodeBinary(src []byte) error {
	return (*InetArray)(dst).DecodeBinary(src)
}

func (src *CidrArray) EncodeText(w io.Writer) (bool, error) {
	return (*InetArray)(src).EncodeText(w)
}

func (src *CidrArray) EncodeBinary(w io.Writer) (bool, error) {
	return (*InetArray)(src).encodeBinary(w, CidrOid)
}
