package pgproto3

import (
	"bytes"
	"encoding/json"
)

type PasswordMessage struct {
	Password string
}

func (*PasswordMessage) Frontend() {}

func (dst *PasswordMessage) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.Password = string(b[:len(b)-1])

	return nil
}

func (src *PasswordMessage) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}
	buf.WriteByte('p')
	buf.Write(bigEndian.Uint32(uint32(4 + len(src.Password) + 1)))
	buf.WriteString(src.Password)
	buf.WriteByte(0)
	return buf.Bytes(), nil
}

func (src *PasswordMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type     string
		Password string
	}{
		Type:     "PasswordMessage",
		Password: src.Password,
	})
}
