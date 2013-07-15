package pgx

// DataRowReader is used by SelectFunc to process incoming rows.
type DataRowReader struct {
	mr              *MessageReader
	fields          []FieldDescription
	currentFieldIdx int
}

func newDataRowReader(mr *MessageReader, fields []FieldDescription) (r *DataRowReader) {
	r = new(DataRowReader)
	r.mr = mr
	r.fields = fields

	fieldCount := int(mr.ReadInt16())
	if fieldCount != len(fields) {
		panic("Row description field count and data row field count do not match")
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
				panic("Unknown format")
			}
		} else {
			return r.mr.ReadByteString(size)
		}
	} else {
		return nil
	}
}
