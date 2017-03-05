package pgtype

import (
	"io"
)

type VarcharArray TextArray

func (dst *VarcharArray) ConvertFrom(src interface{}) error {
	return (*TextArray)(dst).ConvertFrom(src)
}

func (src *VarcharArray) AssignTo(dst interface{}) error {
	return (*TextArray)(src).AssignTo(dst)
}

func (dst *VarcharArray) DecodeText(r io.Reader) error {
	return (*TextArray)(dst).DecodeText(r)
}

func (dst *VarcharArray) DecodeBinary(r io.Reader) error {
	return (*TextArray)(dst).DecodeBinary(r)
}

func (src *VarcharArray) EncodeText(w io.Writer) error {
	return (*TextArray)(src).EncodeText(w)
}

func (src *VarcharArray) EncodeBinary(w io.Writer) error {
	return (*TextArray)(src).encodeBinary(w, VarcharOID)
}
