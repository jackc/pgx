package pgproto3

import (
	"bytes"
	"encoding/binary"

	"github.com/jackc/pgio"
	"github.com/pkg/errors"
)

// Authentication message type constants.
const (
	AuthTypeOk                = 0
	AuthTypeCleartextPassword = 3
	AuthTypeMD5Password       = 5
	AuthTypeSASL              = 10
	AuthTypeSASLContinue      = 11
	AuthTypeSASLFinal         = 12
)

// Authentication is a message sent from the backend during the authentication process.
//
// There are multiple authentication messages that each begin with 'R'. This structure represents all such
// authentication messages.
type Authentication struct {
	Type uint32

	// MD5Password fields
	Salt [4]byte

	// SASL fields
	SASLAuthMechanisms []string

	// SASLContinue and SASLFinal data
	SASLData []byte
}

// Backend identifies this message as sendable by the PostgreSQL backend.
func (*Authentication) Backend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *Authentication) Decode(src []byte) error {
	*dst = Authentication{Type: binary.BigEndian.Uint32(src[:4])}

	switch dst.Type {
	case AuthTypeOk:
	case AuthTypeCleartextPassword:
	case AuthTypeMD5Password:
		copy(dst.Salt[:], src[4:8])
	case AuthTypeSASL:
		authMechanisms := src[4:]
		for len(authMechanisms) > 1 {
			idx := bytes.IndexByte(authMechanisms, 0)
			if idx > 0 {
				dst.SASLAuthMechanisms = append(dst.SASLAuthMechanisms, string(authMechanisms[:idx]))
				authMechanisms = authMechanisms[idx+1:]
			}
		}
	case AuthTypeSASLContinue, AuthTypeSASLFinal:
		dst.SASLData = src[4:]
	default:
		return errors.Errorf("unknown authentication type: %d", dst.Type)
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *Authentication) Encode(dst []byte) []byte {
	dst = append(dst, 'R')
	sp := len(dst)
	dst = pgio.AppendInt32(dst, -1)
	dst = pgio.AppendUint32(dst, src.Type)

	switch src.Type {
	case AuthTypeMD5Password:
		dst = append(dst, src.Salt[:]...)
	case AuthTypeSASL:
		for _, s := range src.SASLAuthMechanisms {
			dst = append(dst, []byte(s)...)
			dst = append(dst, 0)
		}
		dst = append(dst, 0)
	case AuthTypeSASLContinue:
		dst = pgio.AppendInt32(dst, int32(len(src.SASLData)))
		dst = append(dst, src.SASLData...)
	}

	pgio.SetInt32(dst[sp:], int32(len(dst[sp:])))

	return dst
}
