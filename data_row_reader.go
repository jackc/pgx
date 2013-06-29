package pgx

import (
	"fmt"
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

func (r *DataRowReader) ReadValue() interface{} {
	size := r.mr.ReadInt32()
	if size > -1 {
		switch r.fields[0].DataType {
		case oid(16): // bool
			s := r.mr.ReadByteString(size)
			switch s {
			case "t":
				return true
			case "f":
				return false
			default:
				panic(fmt.Sprintf("Received invalid bool: %v", s))
			}
		case oid(20): // int8
			s := r.mr.ReadByteString(size)
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				panic(fmt.Sprintf("Received invalid int8: %v", s))
			}
			return n
		case oid(21): // int2
			s := r.mr.ReadByteString(size)
			n, err := strconv.ParseInt(s, 10, 16)
			if err != nil {
				panic(fmt.Sprintf("Received invalid int2: %v", s))
			}
			return int16(n)
		case oid(23): // int4
			s := r.mr.ReadByteString(size)
			n, err := strconv.ParseInt(s, 10, 32)
			if err != nil {
				panic(fmt.Sprintf("Received invalid int4: %v", s))
			}
			return int32(n)
		case oid(700): // float4
			s := r.mr.ReadByteString(size)
			n, err := strconv.ParseFloat(s, 32)
			if err != nil {
				panic(fmt.Sprintf("Received invalid float4: %v", s))
			}
			return float32(n)
		case oid(701): //float8
			s := r.mr.ReadByteString(size)
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				panic(fmt.Sprintf("Received invalid float8: %v", s))
			}
			return v
		default:
			return r.mr.ReadByteString(size)
		}
	} else {
		return nil
	}
}
