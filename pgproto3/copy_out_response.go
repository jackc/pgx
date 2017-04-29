package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type CopyOutResponse struct {
	OverallFormat     byte
	ColumnFormatCodes []uint16
}

func (*CopyOutResponse) Backend() {}

func (dst *CopyOutResponse) UnmarshalBinary(src []byte) error {
	buf := bytes.NewBuffer(src)

	if buf.Len() < 3 {
		return &invalidMessageFormatErr{messageType: "CopyOutResponse"}
	}

	overallFormat := buf.Next(1)[0]

	columnCount := int(binary.BigEndian.Uint16(buf.Next(2)))
	if buf.Len() != columnCount*2 {
		return &invalidMessageFormatErr{messageType: "CopyOutResponse"}
	}

	columnFormatCodes := make([]uint16, columnCount)
	for i := 0; i < columnCount; i++ {
		columnFormatCodes[i] = binary.BigEndian.Uint16(buf.Next(2))
	}

	*dst = CopyOutResponse{OverallFormat: overallFormat, ColumnFormatCodes: columnFormatCodes}

	return nil
}

func (src *CopyOutResponse) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('H')
	buf.Write(bigEndian.Uint32(uint32(4 + 1 + 2 + 2*len(src.ColumnFormatCodes))))

	buf.Write(bigEndian.Uint16(uint16(len(src.ColumnFormatCodes))))

	for _, fc := range src.ColumnFormatCodes {
		buf.Write(bigEndian.Uint16(fc))
	}

	return buf.Bytes(), nil
}

func (src *CopyOutResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type              string
		ColumnFormatCodes []uint16
	}{
		Type:              "CopyOutResponse",
		ColumnFormatCodes: src.ColumnFormatCodes,
	})
}
