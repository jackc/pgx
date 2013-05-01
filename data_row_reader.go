package pgx

import (
	"strconv"
)

type DataRowReader struct {
	mr     *MessageReader
	fields []FieldDescription
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

func (r *DataRowReader) ReadString() string {
	size := r.mr.ReadInt32()
	if size == -1 {
		panic("Unexpected null")
	}

	return r.mr.ReadByteString(size)
}

func (r *DataRowReader) ReadInt64() int64 {
	size := r.mr.ReadInt32()
	if size == -1 {
		panic("Unexpected null")
	}

	i64, err := strconv.ParseInt(r.mr.ReadByteString(size), 10, 64)
	if err != nil {
		panic("Number too large")
	}
	return i64
}

func (r *DataRowReader) ReadInt32() int32 {
	size := r.mr.ReadInt32()
	if size == -1 {
		panic("Unexpected null")
	}

	i64, err := strconv.ParseInt(r.mr.ReadByteString(size), 10, 32)
	if err != nil {
		panic("Number too large")
	}
	return int32(i64)
}

func (r *DataRowReader) ReadInt16() int16 {
	size := r.mr.ReadInt32()
	if size == -1 {
		panic("Unexpected null")
	}

	i64, err := strconv.ParseInt(r.mr.ReadByteString(size), 10, 16)
	if err != nil {
		panic("Number too large")
	}
	return int16(i64)
}

func (r *DataRowReader) ReadFloat64() float64 {
	size := r.mr.ReadInt32()
	if size == -1 {
		panic("Unexpected null")
	}

	f64, err := strconv.ParseFloat(r.mr.ReadByteString(size), 64)
	if err != nil {
		panic("Number too large")
	}
	return f64
}

func (r *DataRowReader) ReadFloat32() float32 {
	size := r.mr.ReadInt32()
	if size == -1 {
		panic("Unexpected null")
	}

	f64, err := strconv.ParseFloat(r.mr.ReadByteString(size), 32)
	if err != nil {
		panic("Number too large")
	}
	return float32(f64)
}
