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

func (dst *CidrArray) DecodeText(r io.Reader) error {
	return (*InetArray)(dst).DecodeText(r)
}

func (dst *CidrArray) DecodeBinary(r io.Reader) error {
	return (*InetArray)(dst).DecodeBinary(r)
}

func (src *CidrArray) EncodeText(w io.Writer) error {
	return (*InetArray)(src).EncodeText(w)
}

func (src *CidrArray) EncodeBinary(w io.Writer) error {
	return (*InetArray)(src).encodeBinary(w, CidrOID)
}
