package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type Parse struct {
	Name          string
	Query         string
	ParameterOIDs []uint32
}

func (*Parse) Frontend() {}

func (dst *Parse) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.Name = string(b[:len(b)-1])

	b, err = buf.ReadBytes(0)
	if err != nil {
		return err
	}
	dst.Query = string(b[:len(b)-1])

	if buf.Len() < 2 {
		return &invalidMessageFormatErr{messageType: "Parse"}
	}
	parameterOIDCount := int(binary.BigEndian.Uint16(buf.Next(2)))

	for i := 0; i < parameterOIDCount; i++ {
		if buf.Len() < 4 {
			return &invalidMessageFormatErr{messageType: "Parse"}
		}
		dst.ParameterOIDs = append(dst.ParameterOIDs, binary.BigEndian.Uint32(buf.Next(4)))
	}

	return nil
}

func (src *Parse) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('P')
	buf.Write(bigEndian.Uint32(0))

	buf.WriteString(src.Name)
	buf.WriteByte(0)
	buf.WriteString(src.Query)
	buf.WriteByte(0)

	buf.Write(bigEndian.Uint16(uint16(len(src.ParameterOIDs))))

	for _, v := range src.ParameterOIDs {
		buf.Write(bigEndian.Uint32(v))
	}

	binary.BigEndian.PutUint32(buf.Bytes()[1:5], uint32(buf.Len()-1))

	return buf.Bytes(), nil
}

func (src *Parse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type          string
		Name          string
		Query         string
		ParameterOIDs []uint32
	}{
		Type:          "Parse",
		Name:          src.Name,
		Query:         src.Query,
		ParameterOIDs: src.ParameterOIDs,
	})
}
