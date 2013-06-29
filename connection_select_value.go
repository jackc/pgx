package pgx

import (
	"fmt"
	"strconv"
)

func (c *Connection) SelectValue(sql string) (v interface{}, err error) {
	onDataRow := func(r *DataRowReader) error {
		size := r.mr.ReadInt32()
		if size > -1 {
			switch r.fields[0].DataType {
			case oid(16): // bool
				s := r.mr.ReadByteString(size)
				switch s {
				case "t":
					v = true
				case "f":
					v = false
				default:
					fmt.Errorf("Received invalid bool: %v", s)
				}
			case oid(20): // int8
				s := r.mr.ReadByteString(size)
				v, err = strconv.ParseInt(s, 10, 64)
				if err != nil {
					fmt.Errorf("Received invalid int8: %v", s)
				}
			case oid(21): // int2
				s := r.mr.ReadByteString(size)
				var n int64
				n, err = strconv.ParseInt(s, 10, 16)
				if err != nil {
					fmt.Errorf("Received invalid int2: %v", s)
				}
				v = int16(n)
			case oid(23): // int4
				s := r.mr.ReadByteString(size)
				var n int64
				n, err = strconv.ParseInt(s, 10, 32)
				if err != nil {
					fmt.Errorf("Received invalid int4: %v", s)
				}
				v = int32(n)
			case oid(700): // float4
				s := r.mr.ReadByteString(size)
				var n float64
				n, err = strconv.ParseFloat(s, 32)
				if err != nil {
					fmt.Errorf("Received invalid float4: %v", s)
				}
				v = float32(n)
			case oid(701): //float8
				s := r.mr.ReadByteString(size)
				v, err = strconv.ParseFloat(s, 64)
				if err != nil {
					fmt.Errorf("Received invalid float8: %v", s)
				}
			default:
				v = r.mr.ReadByteString(size)
			}
		} else {
			v = nil
		}
		return nil
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}
