package pgx

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/internal/anynil"
	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgtype"
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

func convertSimpleArgument(m *pgtype.Map, arg interface{}) (interface{}, error) {
	if anynil.Is(arg) {
		return nil, nil
	}

	switch arg := arg.(type) {
	case driver.Valuer:
		return arg.Value()
	case float32:
		return float64(arg), nil
	case float64:
		return arg, nil
	case bool:
		return arg, nil
	case time.Duration:
		return fmt.Sprintf("%d microsecond", int64(arg)/1000), nil
	case time.Time:
		return arg, nil
	case string:
		return arg, nil
	case []byte:
		return arg, nil
	case int8:
		return int64(arg), nil
	case int16:
		return int64(arg), nil
	case int32:
		return int64(arg), nil
	case int64:
		return arg, nil
	case int:
		return int64(arg), nil
	case uint8:
		return int64(arg), nil
	case uint16:
		return int64(arg), nil
	case uint32:
		return int64(arg), nil
	case uint64:
		if arg > math.MaxInt64 {
			return nil, fmt.Errorf("arg too big for int64: %v", arg)
		}
		return int64(arg), nil
	case uint:
		if uint64(arg) > math.MaxInt64 {
			return nil, fmt.Errorf("arg too big for int64: %v", arg)
		}
		return int64(arg), nil
	}

	if _, found := m.TypeForValue(arg); found {
		buf, err := m.Encode(0, TextFormatCode, arg, nil)
		if err != nil {
			return nil, err
		}
		if buf == nil {
			return nil, nil
		}
		return string(buf), nil
	}

	refVal := reflect.ValueOf(arg)
	if refVal.Kind() == reflect.Ptr {
		arg = refVal.Elem().Interface()
		return convertSimpleArgument(m, arg)
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		return convertSimpleArgument(m, strippedArg)
	}
	return nil, SerializationError(fmt.Sprintf("Cannot encode %T in simple protocol - %T must implement driver.Valuer, pgtype.TextEncoder, or be a native type", arg, arg))
}

func encodeCopyValue(m *pgtype.Map, buf []byte, oid uint32, arg interface{}) ([]byte, error) {
	if anynil.Is(arg) {
		return pgio.AppendInt32(buf, -1), nil
	}

	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	argBuf, err := m.Encode(oid, BinaryFormatCode, arg, buf)
	if err != nil {
		return nil, err
	}
	if argBuf != nil {
		buf = argBuf
		pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
	}
	return buf, nil
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

func evaluateDriverValuers(args []interface{}) ([]interface{}, error) {
	for i, arg := range args {
		switch arg := arg.(type) {
		case driver.Valuer:
			v, err := arg.Value()
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
	}
	return args, nil
}
