package pgtype

import (
	"database/sql/driver"
	"fmt"
)

// type FieldDescription interface {
// 	Name() string
// 	Table() uint32
// 	AttributeNumber() int16
// 	DataType() uint32
// 	DataTypeSize() int16
// 	DataTypeName() string
// 	Modifier() int32
// 	FormatCode() int16
// }

// Remember need to delegate for server controlled format like inet

// Or separate interfaces for raw bytes and preprocessed by pgx?

// Or interface{} like database/sql - and just pre-process into more things

// type ScannerV3 interface {
// 	ScanPgxV3(fieldDescription FieldDescription, src interface{}) error
// }

// // Encoders could also return interface{} to delegate to internal pgx

// type TextEncoderV3 interface {
// 	EncodeTextPgxV3(oid uint32) (interface{}, error)
// }

// type BinaryEncoderV3 interface {
// 	EncodeBinaryPgxV3(oid uint32) (interface{}, error)
// }

// const (
// 	Int4OID = 23
// )

type Status byte

const (
	Undefined Status = iota
	Null
	Present
)

func (s Status) String() string {
	switch s {
	case Undefined:
		return "Undefined"
	case Null:
		return "Null"
	case Present:
		return "Present"
	}

	return "Invalid status"
}

type Int32Box struct {
	Value2 int32
	Status Status
}

func (s *Int32Box) ScanPgxV3(fieldDescription interface{}, src interface{}) error {
	switch v := src.(type) {
	case int64:
		s.Value2 = int32(v)
		s.Status = Present
	default:
		return fmt.Errorf("cannot scan %v (%T)", v, v)
	}

	return nil
}

func (s *Int32Box) Scan(src interface{}) error {
	switch v := src.(type) {
	case int64:
		s.Value2 = int32(v)
		s.Status = Present
		// TODO - should this have to accept all integer types?
	case int32:
		s.Value2 = int32(v)
		s.Status = Present
	default:
		return fmt.Errorf("cannot scan %v (%T)", v, v)
	}

	return nil
}

func (s Int32Box) Value() (driver.Value, error) {
	// if !n.Valid {

	//   return nil, nil

	// }

	return int64(s.Value2), nil

}

type StringBox struct {
	Value  string
	Status Status
}

func (s *StringBox) ScanPgxV3(fieldDescription interface{}, src interface{}) error {
	switch v := src.(type) {
	case string:
		s.Value = v
		s.Status = Present
	case []byte:
		s.Value = string(v)
		s.Status = Present
	default:
		return fmt.Errorf("cannot scan %v (%T)", v, v)
	}

	return nil
}

func (s *StringBox) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		s.Value = v
		s.Status = Present
	case []byte:
		s.Value = string(v)
		s.Status = Present
	default:
		return fmt.Errorf("cannot scan %v (%T)", v, v)
	}

	return nil
}
