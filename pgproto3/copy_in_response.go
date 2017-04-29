package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type CopyInResponse struct {
	OverallFormat     byte
	ColumnFormatCodes []uint16
}

func (*CopyInResponse) Backend() {}

func (dst *CopyInResponse) Decode(src []byte) error {
	buf := bytes.NewBuffer(src)

	if buf.Len() < 3 {
		return &invalidMessageFormatErr{messageType: "CopyInResponse"}
	}

	overallFormat := buf.Next(1)[0]

	columnCount := int(binary.BigEndian.Uint16(buf.Next(2)))
	if buf.Len() != columnCount*2 {
		return &invalidMessageFormatErr{messageType: "CopyInResponse"}
	}

	columnFormatCodes := make([]uint16, columnCount)
	for i := 0; i < columnCount; i++ {
		columnFormatCodes[i] = binary.BigEndian.Uint16(buf.Next(2))
	}

	*dst = CopyInResponse{OverallFormat: overallFormat, ColumnFormatCodes: columnFormatCodes}

	return nil
}

func (src *CopyInResponse) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('G')
	buf.Write(bigEndian.Uint32(uint32(4 + 1 + 2 + 2*len(src.ColumnFormatCodes))))

	buf.Write(bigEndian.Uint16(uint16(len(src.ColumnFormatCodes))))

	for _, fc := range src.ColumnFormatCodes {
		buf.Write(bigEndian.Uint16(fc))
	}

	return buf.Bytes(), nil
}

func (src *CopyInResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type              string
		ColumnFormatCodes []uint16
	}{
		Type:              "CopyInResponse",
		ColumnFormatCodes: src.ColumnFormatCodes,
	})
}
