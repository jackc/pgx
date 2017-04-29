package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type ParameterStatus struct {
	Name  string
	Value string
}

func (*ParameterStatus) Backend() {}

func (dst *ParameterStatus) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	name := string(b[:len(b)-1])

	b, err = buf.ReadBytes(0)
	if err != nil {
		return err
	}
	value := string(b[:len(b)-1])

	*dst = ParameterStatus{Name: name, Value: value}
	return nil
}

func (src *ParameterStatus) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('S')
	buf.Write(bigEndian.Uint32(0))

	buf.WriteString(src.Name)
	buf.WriteByte(0)
	buf.WriteString(src.Value)
	buf.WriteByte(0)

	binary.BigEndian.PutUint32(buf.Bytes()[1:5], uint32(buf.Len()-1))

	return buf.Bytes(), nil
}

func (ps *ParameterStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string
		Name  string
		Value string
	}{
		Type:  "ParameterStatus",
		Name:  ps.Name,
		Value: ps.Value,
	})
}
