package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type BackendKeyData struct {
	ProcessID uint32
	SecretKey uint32
}

func (*BackendKeyData) Backend() {}

func (dst *BackendKeyData) UnmarshalBinary(src []byte) error {
	if len(src) != 8 {
		return &invalidMessageLenErr{messageType: "BackendKeyData", expectedLen: 8, actualLen: len(src)}
	}

	dst.ProcessID = binary.BigEndian.Uint32(src[:4])
	dst.SecretKey = binary.BigEndian.Uint32(src[4:])

	return nil
}

func (src *BackendKeyData) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}
	buf.WriteByte('K')
	buf.Write(bigEndian.Uint32(12))
	buf.Write(bigEndian.Uint32(src.ProcessID))
	buf.Write(bigEndian.Uint32(src.SecretKey))
	return buf.Bytes(), nil
}

func (src *BackendKeyData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string
		ProcessID uint32
		SecretKey uint32
	}{
		Type:      "BackendKeyData",
		ProcessID: src.ProcessID,
		SecretKey: src.SecretKey,
	})
}
