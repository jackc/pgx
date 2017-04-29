package pgproto3

import (
	"bytes"
	"encoding/json"
)

type Query struct {
	String string
}

func (*Query) Frontend() {}

func (dst *Query) UnmarshalBinary(src []byte) error {
	i := bytes.IndexByte(src, 0)
	if i != len(src)-1 {
		return &invalidMessageFormatErr{messageType: "Query"}
	}

	dst.String = string(src[:i])

	return nil
}

func (src *Query) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}
	buf.WriteByte('Q')
	buf.Write(bigEndian.Uint32(uint32(4 + len(src.String) + 1)))
	buf.WriteString(src.String)
	buf.WriteByte(0)
	return buf.Bytes(), nil
}

func (src *Query) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string
		String string
	}{
		Type:   "Query",
		String: src.String,
	})
}
