package pgtype

import (
	errors "golang.org/x/xerrors"
)

type composite struct {
	fields []Value
	status Status
}

// helper struct to act both as a scanning target and query argument
type rowValue struct {
	args []interface{}
}

// Row helper function builds a value which can be both used to
// "assemble" composite quiery arguments and to scan results back.
//
// When passed as an argument to query, values from Row args will
// be assigned to corresponding fields in a composite type and a single
// composite type will be passed to the PostgreSQL. Composite type need
// to be registered in ConnInfo first. This is required so that pgx
// can know which SQL types to use when constructing SQL composite argument
//
// When passed to Scan individual fields from composite query result
// are assigned to corresponding Row arguments. First argument MUST
// be of type *bool to flag when NULL value received. So total number
// of Row arguments, when passed to Scan should be number of composite
// fields you expect to read + 1
func Row(fields ...interface{}) rowValue {
	return rowValue{fields}
}

// Composite types is meant to be passed to ConnInfo.RegisterDataType only,
// so it is made private on purpose. Once registered, it allows Row
// function to correctly pass query arguments.
func Composite(fields ...Value) *composite {
	return &composite{fields, Undefined}
}

func (src composite) Get() interface{} {
	switch src.status {
	case Present:
		return src
	case Null:
		return nil
	default:
		return src.status
	}
}

// Set is called internally when passing query arguments.
// Only valid src is a result of pgtype.Row() or nil
func (dst *composite) Set(src interface{}) error {
	if src == nil {
		*dst = composite{status: Null}
		return nil
	}

	switch value := src.(type) {
	case rowValue:
		if len(value.args) != len(dst.fields) {
			return errors.Errorf("Number of fields don't match. Composite has %d fields", len(dst.fields))
		}
		for i, v := range value.args {
			if err := dst.fields[i].Set(v); err != nil {
				return err
			}
		}
		dst.status = Present
	default:
		return errors.Errorf("Use pgtype.Row() as query parameter")
	}

	return nil
}

// AssignTo is never called on composite value directly, it is here
// to satisfy Valuer interface
func (src composite) AssignTo(dst interface{}) error {
	return errors.New("BUG: should never be called, because pgtype.composite doesn't support decoding")
}

func (src composite) EncodeBinary(ci *ConnInfo, buf []byte) (newBuf []byte, err error) {
	return EncodeRow(ci, buf, src.fields...)
}

// DecodeBinary here is just to make pgx use binary result format by default.
// Users should be using Row function or their own types to scan composites
func (src composite) DecodeBinary(ci *ConnInfo, buf []byte) (err error) {
	return errors.New("Pass pgtype.Row() to Scan to deconstruct Composite")
}

// Row method creates composite BinaryEncoder. It's main purpose
// is to build composite query argument inplace without registering
// pgtype.Composite in ConnInfo first
func (src composite) Row(values ...interface{}) BinaryEncoderFunc {
	return func(ci *ConnInfo, buf []byte) ([]byte, error) {
		if len(values) != len(src.fields) {
			return nil, errors.Errorf("Number of fields don't match. Composite has %d fields", len(src.fields))
		}
		for i, v := range values {
			if err := src.fields[i].Set(v); err != nil {
				return nil, err
			}
		}
		src.status = Present
		return src.EncodeBinary(ci, buf)
	}
}

// DecodeBinary is called when pgtype.Row() is passed to Scan() to
// deconstruct composite value
func (r rowValue) DecodeBinary(ci *ConnInfo, src []byte) error {
	if len(r.args) == 0 {
		return errors.New("pgtype.Row must have 'isNull *bool' as a first argument when used in Scan")
	}

	isNull, ok := r.args[0].(*bool)
	if !ok {
		return errors.New("pgtype.Row must have 'isNull *bool' as a first argument when used in Scan")
	}
	args := r.args[1:]

	var record Record
	if err := record.DecodeBinary(ci, src); err != nil {
		return err
	}

	if record.Status == Null {
		*isNull = true
		return nil
	}

	if len(record.Fields) != len(args) {
		return errors.Errorf("SQL composite can't be read, 'pgtype.Row' has wrong field cout. %d != %d", len(record.Fields), len(args))
	}

	for i, f := range record.Fields {
		if err := f.AssignTo(args[i]); err != nil {
			return err
		}
	}
	return nil
}
