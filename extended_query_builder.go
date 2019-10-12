package pgx

import (
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/jackc/pgtype"
)

type extendedQueryBuilder struct {
	paramValues     [][]byte
	paramValueBytes []byte
	paramFormats    []int16
	resultFormats   []int16

	resetCount int
}

func (eqb *extendedQueryBuilder) AppendParam(ci *pgtype.ConnInfo, oid uint32, arg interface{}) error {
	f := chooseParameterFormatCode(ci, oid, arg)
	eqb.paramFormats = append(eqb.paramFormats, f)

	v, err := eqb.encodeExtendedParamValue(ci, oid, arg)
	if err != nil {
		return err
	}
	eqb.paramValues = append(eqb.paramValues, v)

	return nil
}

func (eqb *extendedQueryBuilder) AppendResultFormat(f int16) {
	eqb.resultFormats = append(eqb.resultFormats, f)
}

func (eqb *extendedQueryBuilder) Reset() {
	eqb.paramValues = eqb.paramValues[0:0]
	eqb.paramValueBytes = eqb.paramValueBytes[0:0]
	eqb.paramFormats = eqb.paramFormats[0:0]
	eqb.resultFormats = eqb.resultFormats[0:0]

	eqb.resetCount += 1

	// Every so often shrink our reserved memory if it is abnormally high
	if eqb.resetCount%128 == 0 {
		if cap(eqb.paramValues) > 64 {
			eqb.paramValues = make([][]byte, 0, cap(eqb.paramValues)/2)
		}

		if cap(eqb.paramValueBytes) > 256 {
			eqb.paramValueBytes = make([]byte, 0, cap(eqb.paramValueBytes)/2)
		}

		if cap(eqb.paramFormats) > 64 {
			eqb.paramFormats = make([]int16, 0, cap(eqb.paramFormats)/2)
		}
		if cap(eqb.resultFormats) > 64 {
			eqb.resultFormats = make([]int16, 0, cap(eqb.resultFormats)/2)
		}
	}

}

func (eqb *extendedQueryBuilder) encodeExtendedParamValue(ci *pgtype.ConnInfo, oid uint32, arg interface{}) ([]byte, error) {
	if arg == nil {
		return nil, nil
	}

	refVal := reflect.ValueOf(arg)
	argIsPtr := refVal.Kind() == reflect.Ptr

	if argIsPtr && refVal.IsNil() {
		return nil, nil
	}

	if eqb.paramValueBytes == nil {
		eqb.paramValueBytes = make([]byte, 0, 128)
	}

	var err error
	var buf []byte
	pos := len(eqb.paramValueBytes)

	switch arg := arg.(type) {
	case pgtype.BinaryEncoder:
		buf, err = arg.EncodeBinary(ci, eqb.paramValueBytes)
		if err != nil {
			return nil, err
		}
		if buf == nil {
			return nil, nil
		}
		eqb.paramValueBytes = buf
		return eqb.paramValueBytes[pos:], nil
	case pgtype.TextEncoder:
		buf, err = arg.EncodeText(ci, eqb.paramValueBytes)
		if err != nil {
			return nil, err
		}
		if buf == nil {
			return nil, nil
		}
		eqb.paramValueBytes = buf
		return eqb.paramValueBytes[pos:], nil
	case string:
		return []byte(arg), nil
	}

	if argIsPtr {
		// We have already checked that arg is not pointing to nil,
		// so it is safe to dereference here.
		arg = refVal.Elem().Interface()
		return eqb.encodeExtendedParamValue(ci, oid, arg)
	}

	if dt, ok := ci.DataTypeForOID(oid); ok {
		value := dt.Value
		err := value.Set(arg)
		if err != nil {
			{
				if arg, ok := arg.(driver.Valuer); ok {
					v, err := callValuerValue(arg)
					if err != nil {
						return nil, err
					}
					return eqb.encodeExtendedParamValue(ci, oid, v)
				}
			}

			return nil, err
		}

		return eqb.encodeExtendedParamValue(ci, oid, value)
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		return eqb.encodeExtendedParamValue(ci, oid, strippedArg)
	}
	return nil, SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
}
