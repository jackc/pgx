package pgproto3

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/internal/pgio"
)

const cancelRequestCode = 80877102

type CancelRequest struct {
	ProcessID uint32
	SecretKey []byte
}

// Frontend identifies this message as sendable by a PostgreSQL frontend.
func (*CancelRequest) Frontend() {}

func (dst *CancelRequest) Decode(src []byte) error {
	if len(src) < 12 {
		return errors.New("cancel request too short")
	}
	if len(src) > 264 {
		return errors.New("cancel request too long")
	}

	requestCode := binary.BigEndian.Uint32(src)
	if requestCode != cancelRequestCode {
		return errors.New("bad cancel request code")
	}

	dst.ProcessID = binary.BigEndian.Uint32(src[4:])
	dst.SecretKey = make([]byte, len(src)-8)
	copy(dst.SecretKey, src[8:])

	return nil
}

// Encode encodes src into dst. dst will include the 4 byte message length.
func (src *CancelRequest) Encode(dst []byte) ([]byte, error) {
	if len(src.SecretKey) > 256 {
		return nil, errors.New("secret key too long")
	}
	msgLen := int32(12 + len(src.SecretKey))
	dst = pgio.AppendInt32(dst, msgLen)
	dst = pgio.AppendInt32(dst, cancelRequestCode)
	dst = pgio.AppendUint32(dst, src.ProcessID)
	dst = append(dst, src.SecretKey...)
	return dst, nil
}

// MarshalJSON implements encoding/json.Marshaler.
func (src CancelRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string
		ProcessID uint32
		SecretKey string
	}{
		Type:      "CancelRequest",
		ProcessID: src.ProcessID,
		SecretKey: hex.EncodeToString(src.SecretKey),
	})
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (dst *CancelRequest) UnmarshalJSON(data []byte) error {
	var msg struct {
		ProcessID uint32
		SecretKey string
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	dst.ProcessID = msg.ProcessID
	secretKey, err := hex.DecodeString(msg.SecretKey)
	if err != nil {
		return err
	}
	dst.SecretKey = secretKey
	return nil
}
