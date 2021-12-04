package pgtype

import "fmt"

func (ACLItem) BinaryFormatSupported() bool {
	return true
}

func (ACLItem) TextFormatSupported() bool {
	return true
}

func (ACLItem) PreferredFormat() int16 {
	return TextFormatCode
}

func (dst *ACLItem) DecodeResult(ci *ConnInfo, oid uint32, format int16, src []byte) error {
	switch format {
	case BinaryFormatCode:
		return fmt.Errorf("binary format not supported for %T", dst)
	case TextFormatCode:
		return dst.DecodeText(ci, src)
	}
	return fmt.Errorf("unknown format code %d", format)
}

func (src ACLItem) EncodeParam(ci *ConnInfo, oid uint32, format int16, buf []byte) (newBuf []byte, err error) {
	switch format {
	case BinaryFormatCode:
		return nil, fmt.Errorf("binary format not supported for %T", src)
	case TextFormatCode:
		return src.EncodeText(ci, buf)
	}
	return nil, fmt.Errorf("unknown format code %d", format)
}
