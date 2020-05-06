package pgtype

import (
	"github.com/jackc/pgtype/binary"
	errors "golang.org/x/xerrors"
)

type Composite struct {
	fields []Value
	Status Status
}

// NewComposite creates a Composite object, which acts as a "schema" for
// SQL composite values.
// To pass Composite as SQL parameter first set it's fields, either by
// passing initialized Value{} instances to NewComposite or by calling
// SetFields method
// To read composite fields back pass result of Scan() method
// to query Scan function.
func NewComposite(fields ...Value) *Composite {
	return &Composite{fields, Present}
}

func (src Composite) Get() interface{} {
	switch src.Status {
	case Present:
		return src
	case Null:
		return nil
	default:
		return src.Status
	}
}

// Set is called internally when passing query arguments.
func (dst *Composite) Set(src interface{}) error {
	if src == nil {
		*dst = Composite{Status: Null}
		return nil
	}

	switch value := src.(type) {
	case []Value:
		if len(value) != len(dst.fields) {
			return errors.Errorf("Number of fields don't match. Composite has %d fields", len(dst.fields))
		}
		for i, v := range value {
			if err := dst.fields[i].Set(v); err != nil {
				return err
			}
		}
		dst.Status = Present
	default:
		return errors.Errorf("Can not convert %v to Composite", src)
	}

	return nil
}

// AssignTo should never be called on composite value directly
func (src Composite) AssignTo(dst interface{}) error {
	return errors.New("Pass Composite.Scan() to deconstruct composite")
}

func (src Composite) EncodeBinary(ci *ConnInfo, buf []byte) (newBuf []byte, err error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}
	return EncodeRow(ci, buf, src.fields...)
}

// DecodeBinary implements BinaryDecoder interface.
// Opposite to Record, fields in a composite act as a "schema"
// and decoding fails if SQL value can't be assigned due to
// type mismatch
func (dst *Composite) DecodeBinary(ci *ConnInfo, buf []byte) (err error) {
	if buf == nil {
		dst.Status = Null
		return nil
	}

	fieldIter, fieldCount, err := binary.NewRecordFieldIterator(buf)
	if err != nil {
		return err
	} else if len(dst.fields) != fieldCount {
		return errors.Errorf("SQL composite can't be read, field count mismatch. expected %d , found %d", len(dst.fields), fieldCount)
	}

	_, fieldBytes, eof, err := fieldIter.Next()

	for i := 0; !eof; i++ {
		if err != nil {
			return err
		}

		binaryDecoder, ok := dst.fields[i].(BinaryDecoder)
		if !ok {
			return errors.New("Composite field doesn't support binary protocol")
		}

		if err = binaryDecoder.DecodeBinary(ci, fieldBytes); err != nil {
			return err
		}

		_, fieldBytes, eof, err = fieldIter.Next()
	}
	dst.Status = Present

	return nil
}

// Scan is a helper function to perform "nested" scan of
// a composite value when scanning a query result row.
// isNull is set if scanned value is NULL
// Rest of arguments are set in the order of fields in the composite
//
// Use of Scan method doesn't modify original composite
func (src Composite) Scan(isNull *bool, dst ...interface{}) BinaryDecoderFunc {
	return func(ci *ConnInfo, buf []byte) error {
		if err := src.DecodeBinary(ci, buf); err != nil {
			return err
		}

		if src.Status == Null {
			*isNull = true
			return nil
		}

		for i, f := range src.fields {
			if err := f.AssignTo(dst[i]); err != nil {
				return err
			}
		}
		return nil
	}
}

// SetFields sets Composite's fields to corresponding values
func (dst *Composite) SetFields(values ...interface{}) error {
	if len(values) != len(dst.fields) {
		return errors.Errorf("Number of fields don't match. Composite has %d fields", len(dst.fields))
	}
	for i, v := range values {
		if err := dst.fields[i].Set(v); err != nil {
			return err
		}
	}
	dst.Status = Present
	return nil
}
