package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
)

type DataRow struct {
	Values [][]byte
}

func (*DataRow) Backend() {}

func (dst *DataRow) UnmarshalBinary(src []byte) error {
	buf := bytes.NewBuffer(src)

	if buf.Len() < 2 {
		return &invalidMessageFormatErr{messageType: "DataRow"}
	}
	fieldCount := int(binary.BigEndian.Uint16(buf.Next(2)))

	dst.Values = make([][]byte, fieldCount)

	for i := 0; i < fieldCount; i++ {
		if buf.Len() < 4 {
			return &invalidMessageFormatErr{messageType: "DataRow"}
		}

		msgSize := int(int32(binary.BigEndian.Uint32(buf.Next(4))))

		// null
		if msgSize == -1 {
			continue
		}

		value := make([]byte, msgSize)
		_, err := buf.Read(value)
		if err != nil {
			return err
		}

		dst.Values[i] = value
	}

	return nil
}

func (src *DataRow) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('D')
	buf.Write(bigEndian.Uint32(0))

	buf.Write(bigEndian.Uint16(uint16(len(src.Values))))

	for _, v := range src.Values {
		if v == nil {
			buf.Write(bigEndian.Int32(-1))
			continue
		}

		buf.Write(bigEndian.Int32(int32(len(v))))
		buf.Write(v)
	}

	binary.BigEndian.PutUint32(buf.Bytes()[1:5], uint32(buf.Len()-1))

	return buf.Bytes(), nil
}

func (src *DataRow) MarshalJSON() ([]byte, error) {
	formattedValues := make([]map[string]string, len(src.Values))
	for i, v := range src.Values {
		if v == nil {
			continue
		}

		var hasNonPrintable bool
		for _, b := range v {
			if b < 32 {
				hasNonPrintable = true
				break
			}
		}

		if hasNonPrintable {
			formattedValues[i] = map[string]string{"binary": hex.EncodeToString(v)}
		} else {
			formattedValues[i] = map[string]string{"text": string(v)}
		}
	}

	return json.Marshal(struct {
		Type   string
		Values []map[string]string
	}{
		Type:   "DataRow",
		Values: formattedValues,
	})
}
