package pgproto3

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	AuthTypeOk                = 0
	AuthTypeCleartextPassword = 3
	AuthTypeMD5Password       = 5
)

type Authentication struct {
	Type uint32

	// MD5Password fields
	Salt [4]byte
}

func (*Authentication) Backend() {}

func (dst *Authentication) UnmarshalBinary(src []byte) error {
	*dst = Authentication{Type: binary.BigEndian.Uint32(src[:4])}

	switch dst.Type {
	case AuthTypeOk:
	case AuthTypeCleartextPassword:
	case AuthTypeMD5Password:
		copy(dst.Salt[:], src[4:8])
	default:
		return fmt.Errorf("unknown authentication type: %d", dst.Type)
	}

	return nil
}

func (src *Authentication) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}
	buf.WriteByte('R')
	buf.Write(bigEndian.Uint32(0))
	buf.Write(bigEndian.Uint32(src.Type))

	switch src.Type {
	case AuthTypeMD5Password:
		buf.Write(src.Salt[:])
	}

	binary.BigEndian.PutUint32(buf.Bytes()[1:5], uint32(buf.Len()-1))

	return buf.Bytes(), nil
}
