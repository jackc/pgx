package pgtype

import (
	"encoding/binary"
	"reflect"

	errors "golang.org/x/xerrors"
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
	if src == nil {
		*dst = Record{Status: Null}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case []Value:
		*dst = Record{Fields: value, Status: Present}
	default:
		return errors.Errorf("cannot convert %v to Record", src)
	}

	return nil
}

func (dst Record) Get() interface{} {
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
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *[]Value:
			*v = make([]Value, len(src.Fields))
			copy(*v, src.Fields)
			return nil
		case *[]interface{}:
			*v = make([]interface{}, len(src.Fields))
			for i := range *v {
				(*v)[i] = src.Fields[i].Get()
			}
			return nil
		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
			return errors.Errorf("unable to assign to %T", dst)
		}
	case Null:
		return NullAssignTo(dst)
	}

	return errors.Errorf("cannot decode %#v into %T", src, dst)
}

type fieldIter struct {
	rp         int
	fieldCount int
	src        []byte
}

func newFieldIterator(src []byte) (fieldIter, error) {
	rp := 0
	if len(src[rp:]) < 4 {
		return fieldIter{}, errors.Errorf("Record incomplete %v", src)
	}

	fieldCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	return fieldIter{
		rp:         rp,
		fieldCount: fieldCount,
		src:        src,
	}, nil
}

func (fi *fieldIter) next() (fieldOID uint32, buf []byte, eof bool, err error) {
	if fi.rp == len(fi.src) {
		eof = true
		return
	}

	if len(fi.src[fi.rp:]) < 8 {
		err = errors.Errorf("Record incomplete %v", fi.src)
		return
	}
	fieldOID = binary.BigEndian.Uint32(fi.src[fi.rp:])
	fi.rp += 4

	fieldLen := int(int32(binary.BigEndian.Uint32(fi.src[fi.rp:])))
	fi.rp += 4

	if fieldLen >= 0 {
		if len(fi.src[fi.rp:]) < fieldLen {
			err = errors.Errorf("Record incomplete rp=%d src=%v", fi.rp, fi.src)
			return
		}
		buf = fi.src[fi.rp : fi.rp+fieldLen]
		fi.rp += fieldLen
	}

	return
}

func prepareNewBinaryDecoder(ci *ConnInfo, fieldOID uint32, v *Value) (BinaryDecoder, error) {
	var binaryDecoder BinaryDecoder

	if dt, ok := ci.DataTypeForOID(fieldOID); ok {
		binaryDecoder, _ = dt.Value.(BinaryDecoder)
	} else {
		return nil, errors.Errorf("unknown oid while decoding record: %v", fieldOID)
	}

	if binaryDecoder == nil {
		return nil, errors.Errorf("no binary decoder registered for: %v", fieldOID)
	}

	// Duplicate struct to scan into
	binaryDecoder = reflect.New(reflect.ValueOf(binaryDecoder).Elem().Type()).Interface().(BinaryDecoder)
	*v = binaryDecoder.(Value)
	return binaryDecoder, nil
}

func (dst *Record) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Record{Status: Null}
		return nil
	}

	fieldIter, err := newFieldIterator(src)
	if err != nil {
		return err
	}

	fields := make([]Value, fieldIter.fieldCount)
	fieldOID, fieldBytes, eof, err := fieldIter.next()

	for i := 0; !eof; i++ {
		if err != nil {
			return err
		}

		binaryDecoder, err := prepareNewBinaryDecoder(ci, fieldOID, &fields[i])
		if err != nil {
			return err
		}

		if err = binaryDecoder.DecodeBinary(ci, fieldBytes); err != nil {
			return err
		}

		fieldOID, fieldBytes, eof, err = fieldIter.next()
	}

	*dst = Record{Fields: fields, Status: Present}

	return nil
}
