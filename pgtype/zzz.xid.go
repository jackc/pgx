package pgtype

import "fmt"

func (XID) BinaryFormatSupported() bool {
	return true
}

func (XID) TextFormatSupported() bool {
	return true
}

func (XID) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (dst *XID) DecodeResult(ci *ConnInfo, oid uint32, format int16, src []byte) error {
	switch format {
	case BinaryFormatCode:
		return dst.DecodeBinary(ci, src)
	case TextFormatCode:
		return dst.DecodeText(ci, src)
	}
	return fmt.Errorf("unknown format code %d", format)
}

func (src XID) EncodeParam(ci *ConnInfo, oid uint32, format int16, buf []byte) (newBuf []byte, err error) {
	switch format {
	case BinaryFormatCode:
		return src.EncodeBinary(ci, buf)
	case TextFormatCode:
		return src.EncodeText(ci, buf)
	}
	return nil, fmt.Errorf("unknown format code %d", format)
}
