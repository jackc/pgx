package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

const (
	TextFormat   = 0
	BinaryFormat = 1
)

type FieldDescription struct {
	Name                 string
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
	DataTypeSize         int16
	TypeModifier         uint32
	Format               int16
}

type RowDescription struct {
	Fields []FieldDescription
}

func (*RowDescription) Backend() {}

func (dst *RowDescription) UnmarshalBinary(src []byte) error {
	buf := bytes.NewBuffer(src)

	if buf.Len() < 2 {
		return &invalidMessageFormatErr{messageType: "RowDescription"}
	}
	fieldCount := int(binary.BigEndian.Uint16(buf.Next(2)))

	*dst = RowDescription{Fields: make([]FieldDescription, fieldCount)}

	for i := 0; i < fieldCount; i++ {
		var fd FieldDescription
		bName, err := buf.ReadBytes(0)
		if err != nil {
			return err
		}
		fd.Name = string(bName[:len(bName)-1])

		// Since buf.Next() doesn't return an error if we hit the end of the buffer
		// check Len ahead of time
		if buf.Len() < 18 {
			return &invalidMessageFormatErr{messageType: "RowDescription"}
		}

		fd.TableOID = binary.BigEndian.Uint32(buf.Next(4))
		fd.TableAttributeNumber = binary.BigEndian.Uint16(buf.Next(2))
		fd.DataTypeOID = binary.BigEndian.Uint32(buf.Next(4))
		fd.DataTypeSize = int16(binary.BigEndian.Uint16(buf.Next(2)))
		fd.TypeModifier = binary.BigEndian.Uint32(buf.Next(4))
		fd.Format = int16(binary.BigEndian.Uint16(buf.Next(2)))

		dst.Fields[i] = fd
	}

	return nil
}

func (src *RowDescription) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('T')
	buf.Write(bigEndian.Uint32(0))

	buf.Write(bigEndian.Uint16(uint16(len(src.Fields))))

	for _, fd := range src.Fields {
		buf.WriteString(fd.Name)
		buf.WriteByte(0)

		buf.Write(bigEndian.Uint32(fd.TableOID))
		buf.Write(bigEndian.Uint16(fd.TableAttributeNumber))
		buf.Write(bigEndian.Uint32(fd.DataTypeOID))
		buf.Write(bigEndian.Uint16(uint16(fd.DataTypeSize)))
		buf.Write(bigEndian.Uint32(fd.TypeModifier))
		buf.Write(bigEndian.Uint16(uint16(fd.Format)))
	}

	binary.BigEndian.PutUint32(buf.Bytes()[1:5], uint32(buf.Len()-1))

	return buf.Bytes(), nil
}

func (src *RowDescription) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string
		Fields []FieldDescription
	}{
		Type:   "RowDescription",
		Fields: src.Fields,
	})
}
