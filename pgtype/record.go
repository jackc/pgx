package pgtype

import (
	"encoding/binary"
	"fmt"
)

// Record is the generic PostgreSQL record type such as is created with the
// "row" function. Record only implements BinaryEncoder and Value. The text
// format output format from PostgreSQL does not include type information and is
// therefore impossible to decode. No encoders are implemented because
// PostgreSQL does not support input of generic records.
type Record struct {
	Fields []Value
	Status Status
}

func (dst *Record) Set(src interface{}) error {
	switch value := src.(type) {
	case []Value:
		*dst = Record{Fields: value, Status: Present}
	default:
		return fmt.Errorf("cannot convert %v to Record", src)
	}

	return nil
}

func (dst *Record) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Fields
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Record) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *[]Value:
		switch src.Status {
		case Present:
			*v = make([]Value, len(src.Fields))
			copy(*v, src.Fields)
		case Null:
			*v = nil
		default:
			return fmt.Errorf("cannot decode %v into %T", src, dst)
		}
	case *[]interface{}:
		switch src.Status {
		case Present:
			*v = make([]interface{}, len(src.Fields))
			for i := range *v {
				(*v)[i] = src.Fields[i].Get()
			}
		case Null:
			*v = nil
		default:
			return fmt.Errorf("cannot decode %v into %T", src, dst)
		}
	default:
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *Record) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Record{Status: Null}
		return nil
	}

	rp := 0

	if len(src[rp:]) < 4 {
		return fmt.Errorf("Record incomplete %v", src)
	}
	fieldCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	fields := make([]Value, fieldCount)

	for i := 0; i < fieldCount; i++ {
		if len(src[rp:]) < 8 {
			return fmt.Errorf("Record incomplete %v", src)
		}
		fieldOid := Oid(binary.BigEndian.Uint32(src[rp:]))
		rp += 4

		fieldLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		var binaryDecoder BinaryDecoder
		if dt, ok := ci.DataTypeForOid(fieldOid); ok {
			if binaryDecoder, ok = dt.Value.(BinaryDecoder); !ok {
				return fmt.Errorf("unknown oid while decoding record: %v", fieldOid)
			}
		}

		var fieldBytes []byte
		if fieldLen >= 0 {
			if len(src[rp:]) < fieldLen {
				return fmt.Errorf("Record incomplete %v", src)
			}
			fieldBytes = src[rp : rp+fieldLen]
			rp += fieldLen
		}

		if err := binaryDecoder.DecodeBinary(ci, fieldBytes); err != nil {
			return err
		}

		fields[i] = binaryDecoder.(Value)
	}

	*dst = Record{Fields: fields, Status: Present}

	return nil
}
