package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type Execute struct {
	Portal  string
	MaxRows uint32
}

func (*Execute) Frontend() {}

func (dst *Execute) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.Portal = string(b[:len(b)-1])

	if buf.Len() < 4 {
		return &invalidMessageFormatErr{messageType: "Execute"}
	}
	dst.MaxRows = binary.BigEndian.Uint32(buf.Next(4))

	return nil
}

func (src *Execute) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('E')
	buf.Write(bigEndian.Uint32(0))

	buf.WriteString(src.Portal)
	buf.WriteByte(0)

	buf.Write(bigEndian.Uint32(src.MaxRows))

	binary.BigEndian.PutUint32(buf.Bytes()[1:5], uint32(buf.Len()-1))

	return buf.Bytes(), nil
}

func (src *Execute) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string
		Portal  string
		MaxRows uint32
	}{
		Type:    "Execute",
		Portal:  src.Portal,
		MaxRows: src.MaxRows,
	})
}
