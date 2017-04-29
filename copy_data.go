package pgproto3

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
)

type CopyData struct {
	Data []byte
}

func (*CopyData) Backend()  {}
func (*CopyData) Frontend() {}

func (dst *CopyData) UnmarshalBinary(src []byte) error {
	dst.Data = make([]byte, len(src))
	copy(dst.Data, src)
	return nil
}

func (src *CopyData) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('d')
	buf.Write(bigEndian.Uint32(uint32(4 + len(src.Data))))
	buf.Write(src.Data)

	return buf.Bytes(), nil
}

func (src *CopyData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Data string
	}{
		Type: "CopyData",
		Data: hex.EncodeToString(src.Data),
	})
}
