package pgproto3

import (
	"bytes"
	"encoding/json"
)

type CommandComplete struct {
	CommandTag string
}

func (*CommandComplete) Backend() {}

func (dst *CommandComplete) Decode(src []byte) error {
	idx := bytes.IndexByte(src, 0)
	if idx != len(src)-1 {
		return &invalidMessageFormatErr{messageType: "CommandComplete"}
	}

	dst.CommandTag = string(src[:idx])

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
