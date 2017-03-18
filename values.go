package pgx

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/pgtype"
)

// PostgreSQL oids for common types
const (
	BoolOid             = 16
	ByteaOid            = 17
	CharOid             = 18
	NameOid             = 19
	Int8Oid             = 20
	Int2Oid             = 21
	Int4Oid             = 23
	TextOid             = 25
	OidOid              = 26
	TidOid              = 27
	XidOid              = 28
	CidOid              = 29
	JsonOid             = 114
	CidrOid             = 650
	CidrArrayOid        = 651
	Float4Oid           = 700
	Float8Oid           = 701
	UnknownOid          = 705
	InetOid             = 869
	BoolArrayOid        = 1000
	Int2ArrayOid        = 1005
	Int4ArrayOid        = 1007
	TextArrayOid        = 1009
	ByteaArrayOid       = 1001
	VarcharArrayOid     = 1015
	Int8ArrayOid        = 1016
	Float4ArrayOid      = 1021
	Float8ArrayOid      = 1022
	AclitemOid          = 1033
	AclitemArrayOid     = 1034
	InetArrayOid        = 1041
	VarcharOid          = 1043
	DateOid             = 1082
	TimestampOid        = 1114
	TimestampArrayOid   = 1115
	DateArrayOid        = 1182
	TimestampTzOid      = 1184
	TimestampTzArrayOid = 1185
	RecordOid           = 2249
	UuidOid             = 2950
	JsonbOid            = 3802
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)
const minInt = -maxInt - 1

// SerializationError occurs on failure to encode or decode a value
type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

// Encode encodes arg into wbuf as the type oid. This allows implementations
// of the Encoder interface to delegate the actual work of encoding to the
// built-in functionality.
func Encode(wbuf *WriteBuf, oid pgtype.Oid, arg interface{}) error {
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
		return Encode(wbuf, oid, v)
	case string:
		return encodeString(wbuf, oid, arg)
	case []byte:
		return encodeByteSlice(wbuf, oid, arg)
	}

	refVal := reflect.ValueOf(arg)

	if refVal.Kind() == reflect.Ptr {
		if refVal.IsNil() {
			wbuf.WriteInt32(-1)
			return nil
		}
		arg = refVal.Elem().Interface()
		return Encode(wbuf, oid, arg)
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
		return Encode(wbuf, oid, strippedArg)
	}
	return SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
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

func decodeText(vr *ValueReader) string {
	if vr.Len() == -1 {
		vr.Fatal(ProtocolError("Cannot decode null into string"))
		return ""
	}

	if vr.Type().FormatCode == BinaryFormatCode {
		vr.Fatal(ProtocolError("cannot decode binary value into string"))
		return ""
	}

	return vr.ReadString(vr.Len())
}

func encodeString(w *WriteBuf, oid pgtype.Oid, value string) error {
	w.WriteInt32(int32(len(value)))
	w.WriteBytes([]byte(value))
	return nil
}

func decodeBytea(vr *ValueReader) []byte {
	if vr.Len() == -1 {
		return nil
	}

	if vr.Type().DataType != ByteaOid {
		vr.Fatal(ProtocolError(fmt.Sprintf("Cannot decode oid %v into []byte", vr.Type().DataType)))
		return nil
	}

	if vr.Type().FormatCode != BinaryFormatCode {
		vr.Fatal(ProtocolError(fmt.Sprintf("Unknown field description format code: %v", vr.Type().FormatCode)))
		return nil
	}

	return vr.ReadBytes(vr.Len())
}

func encodeByteSlice(w *WriteBuf, oid pgtype.Oid, value []byte) error {
	w.WriteInt32(int32(len(value)))
	w.WriteBytes(value)

	return nil
}
