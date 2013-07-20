package pgx

import (
	"fmt"
)

// DataRowReader is used by SelectFunc to process incoming rows.
type DataRowReader struct {
	mr              *MessageReader
	fields          []FieldDescription
	currentFieldIdx int
}

func newDataRowReader(mr *MessageReader, fields []FieldDescription) (r *DataRowReader, err error) {
	r = new(DataRowReader)
	r.mr = mr
	r.fields = fields

	fieldCount := int(mr.ReadInt16())
	if fieldCount != len(fields) {
		return nil, ProtocolError(fmt.Sprintf("Row description field count (%v) and data row field count (%v) do not match", len(fields), fieldCount))
	}

	return
}

// ReadValue returns the next value from the current row.
func (r *DataRowReader) ReadValue() interface{} {
	fieldDescription := r.fields[r.currentFieldIdx]
	r.currentFieldIdx++

	size := r.mr.ReadInt32()
	if size > -1 {
		if vt, present := ValueTranscoders[fieldDescription.DataType]; present {
			switch fieldDescription.FormatCode {
			case 0:
				return vt.DecodeText(r.mr, size)
			case 1:
				return vt.DecodeBinary(r.mr, size)
			default:
				return ProtocolError(fmt.Sprintf("Unknown field description format code: %v", fieldDescription.FormatCode))
			}
		} else {
			return r.mr.ReadString(size)
		}
	} else {
		return nil
	}
}
