package pgx

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

func (r *DataRowReader) ReadValue() interface{} {
	dataType := r.fields[r.currentFieldIdx].DataType
	r.currentFieldIdx++

	size := r.mr.ReadInt32()
	if size > -1 {
		if vt, present := valueTranscoders[dataType]; present {
			return vt.FromText(r.mr, size)
		} else {
			return r.mr.ReadByteString(size)
		}
	} else {
		return nil
	}
}
