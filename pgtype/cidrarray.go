package pgtype

import (
	"io"
)

type CidrArray InetArray

func (dst *CidrArray) ConvertFrom(src interface{}) error {
	return (*InetArray)(dst).ConvertFrom(src)
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
	return (*InetArray)(src).encodeBinary(w, CidrOID)
}
