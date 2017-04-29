package pgproto3

import (
	"bytes"
	"encoding/json"
)

type CommandComplete struct {
	CommandTag string
}

func (*CommandComplete) Backend() {}

func (dst *CommandComplete) UnmarshalBinary(src []byte) error {
	buf := bytes.NewBuffer(src)

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.CommandTag = string(b[:len(b)-1])

	return nil
}

func (src *CommandComplete) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('C')
	buf.Write(bigEndian.Uint32(uint32(4 + len(src.CommandTag) + 1)))

	buf.WriteString(src.CommandTag)
	buf.WriteByte(0)

	return buf.Bytes(), nil
}

func (src *CommandComplete) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type       string
		CommandTag string
	}{
		Type:       "CommandComplete",
		CommandTag: src.CommandTag,
	})
}
