package pgx

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/pgtype"
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

// SerializationError occurs on failure to encode or decode a value
type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

func encodePreparedStatementArgument(wbuf *WriteBuf, oid pgtype.Oid, arg interface{}) error {
	if arg == nil {
		wbuf.WriteInt32(-1)
		return nil
	}

	switch arg := arg.(type) {
	case pgtype.BinaryEncoder:
		buf := &bytes.Buffer{}
		null, err := arg.EncodeBinary(wbuf.conn.ConnInfo, buf)
		if err != nil {
			return err
		}
		if null {
			wbuf.WriteInt32(-1)
		} else {
			wbuf.WriteInt32(int32(buf.Len()))
			wbuf.WriteBytes(buf.Bytes())
		}
		return nil
	case pgtype.TextEncoder:
		buf := &bytes.Buffer{}
		null, err := arg.EncodeText(wbuf.conn.ConnInfo, buf)
		if err != nil {
			return err
		}
		if null {
			wbuf.WriteInt32(-1)
		} else {
			wbuf.WriteInt32(int32(buf.Len()))
			wbuf.WriteBytes(buf.Bytes())
		}
		return nil
	case driver.Valuer:
		v, err := arg.Value()
		if err != nil {
			return err
		}
		return encodePreparedStatementArgument(wbuf, oid, v)
	case string:
		wbuf.WriteInt32(int32(len(arg)))
		wbuf.WriteBytes([]byte(arg))
		return nil
	}

	refVal := reflect.ValueOf(arg)

	if refVal.Kind() == reflect.Ptr {
		if refVal.IsNil() {
			wbuf.WriteInt32(-1)
			return nil
		}
		arg = refVal.Elem().Interface()
		return encodePreparedStatementArgument(wbuf, oid, arg)
	}

	if dt, ok := wbuf.conn.ConnInfo.DataTypeForOid(oid); ok {
		value := dt.Value
		err := value.Set(arg)
		if err != nil {
			return err
		}

		buf := &bytes.Buffer{}
		null, err := value.(pgtype.BinaryEncoder).EncodeBinary(wbuf.conn.ConnInfo, buf)
		if err != nil {
			return err
		}
		if null {
			wbuf.WriteInt32(-1)
		} else {
			wbuf.WriteInt32(int32(buf.Len()))
			wbuf.WriteBytes(buf.Bytes())
		}
		return nil
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		return encodePreparedStatementArgument(wbuf, oid, strippedArg)
	}
	return SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
}

// chooseParameterFormatCode determines the correct format code for an
// argument to a prepared statement. It defaults to TextFormatCode if no
// determination can be made.
func chooseParameterFormatCode(ci *pgtype.ConnInfo, oid pgtype.Oid, arg interface{}) int16 {
	switch arg.(type) {
	case pgtype.BinaryEncoder:
		return BinaryFormatCode
	case string, *string, pgtype.TextEncoder:
		return TextFormatCode
	}

	if dt, ok := ci.DataTypeForOid(oid); ok {
		if _, ok := dt.Value.(pgtype.BinaryEncoder); ok {
			if arg, ok := arg.(driver.Valuer); ok {
				if err := dt.Value.Set(arg); err != nil {
					if value, err := arg.Value(); err == nil {
						if _, ok := value.(string); ok {
							return TextFormatCode
						}
					}
				}
			}

			return BinaryFormatCode
		}
	}

	return TextFormatCode
}

func stripNamedType(val *reflect.Value) (interface{}, bool) {
	switch val.Kind() {
	case reflect.Int:
		convVal := int(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int8:
		convVal := int8(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int16:
		convVal := int16(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int32:
		convVal := int32(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Int64:
		convVal := int64(val.Int())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint:
		convVal := uint(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint8:
		convVal := uint8(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint16:
		convVal := uint16(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint32:
		convVal := uint32(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.Uint64:
		convVal := uint64(val.Uint())
		return convVal, reflect.TypeOf(convVal) != val.Type()
	case reflect.String:
		convVal := val.String()
		return convVal, reflect.TypeOf(convVal) != val.Type()
	}

	return nil, false
}
