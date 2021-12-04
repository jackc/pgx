package pgtype

import "fmt"

func (Bytea) BinaryFormatSupported() bool {
	return true
}

func (Bytea) TextFormatSupported() bool {
	return true
}

func (Bytea) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (dst *Bytea) DecodeResult(ci *ConnInfo, oid uint32, format int16, src []byte) error {
	switch format {
	case BinaryFormatCode:
		return dst.DecodeBinary(ci, src)
	case TextFormatCode:
		return dst.DecodeText(ci, src)
	}
	return fmt.Errorf("unknown format code %d", format)
}

func (src Bytea) EncodeParam(ci *ConnInfo, oid uint32, format int16, buf []byte) (newBuf []byte, err error) {
	switch format {
	case BinaryFormatCode:
		return src.EncodeBinary(ci, buf)
	case TextFormatCode:
		return src.EncodeText(ci, buf)
	}
	return nil, fmt.Errorf("unknown format code %d", format)
}
