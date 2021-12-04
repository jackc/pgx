package pgtype

import "fmt"

func (QChar) BinaryFormatSupported() bool {
	return true
}

func (QChar) TextFormatSupported() bool {
	return true
}

func (QChar) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (dst *QChar) DecodeResult(ci *ConnInfo, oid uint32, format int16, src []byte) error {
	switch format {
	case BinaryFormatCode:
		return dst.DecodeBinary(ci, src)
	case TextFormatCode:
		return fmt.Errorf("text format not supported for %T", dst)
	}
	return fmt.Errorf("unknown format code %d", format)
}

func (src QChar) EncodeParam(ci *ConnInfo, oid uint32, format int16, buf []byte) (newBuf []byte, err error) {
	switch format {
	case BinaryFormatCode:
		return src.EncodeBinary(ci, buf)
	case TextFormatCode:
		return nil, fmt.Errorf("text format not supported for %T", src)
	}
	return nil, fmt.Errorf("unknown format code %d", format)
}
