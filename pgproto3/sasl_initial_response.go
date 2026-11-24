package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/internal/pgio"
)

type SASLInitialResponse struct {
	AuthMechanism string
	Data          []byte
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*SASLInitialResponse) Frontend() {}

// Decode decodes src into dst. src must contain the complete message with the exception of the initial 1 byte message
// type identifier and 4 byte message length.
func (dst *SASLInitialResponse) Decode(src []byte) error {
	*dst = SASLInitialResponse{}

	rp := 0

	idx := bytes.IndexByte(src, 0)
	if idx < 0 {
		return errors.New("invalid SASLInitialResponse")
	}

	dst.AuthMechanism = string(src[rp:idx])
	rp = idx + 1

	if len(src[rp:]) < 4 {
		return errors.New("invalid SASLInitialResponse")
	}

	// The initial response length must be validated against the remaining
	// message size: -1 means the mechanism sends no initial response and
	// the message ends here, and any other value is the exact number of
	// data bytes that follow. Skipping the field without reading it would
	// silently accept truncated data and trailing bytes as SASL data.
	dataLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	switch {
	case dataLen == -1:
		if len(src[rp:]) != 0 {
			return errors.New("invalid SASLInitialResponse")
		}
		dst.Data = nil
	case dataLen < 0 || len(src[rp:]) != dataLen:
		return errors.New("invalid SASLInitialResponse")
	default:
		dst.Data = src[rp : rp+dataLen]
	}

	return nil
}

// Encode encodes src into dst. dst will include the 1 byte message type identifier and the 4 byte message length.
func (src *SASLInitialResponse) Encode(dst []byte) ([]byte, error) {
	dst, sp := beginMessage(dst, 'p')

	dst = append(dst, []byte(src.AuthMechanism)...)
	dst = append(dst, 0)

	dst = pgio.AppendInt32(dst, int32(len(src.Data)))
	dst = append(dst, src.Data...)

	return finishMessage(dst, sp)
}

// MarshalJSON implements encoding/json.Marshaler.
func (src SASLInitialResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type          string
		AuthMechanism string
		Data          string
	}{
		Type:          "SASLInitialResponse",
		AuthMechanism: src.AuthMechanism,
		Data:          string(src.Data),
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *SASLInitialResponse) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" {
		return nil
	}

	var msg struct {
		AuthMechanism string
		Data          string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	dst.AuthMechanism = msg.AuthMechanism
	if msg.Data != "" {
		decoded, err := hex.DecodeString(msg.Data)
		if err != nil {
			return err
		}
		dst.Data = decoded
	}
	return nil
}
