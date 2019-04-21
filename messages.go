package pgx

import (
	"database/sql/driver"
	"math"
	"reflect"
	"time"

	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
)

const (
	copyData      = 'd'
	copyFail      = 'f'
	copyDone      = 'c'
	varHeaderSize = 4
)

type FieldDescription struct {
	Name            string
	Table           pgtype.OID
	AttributeNumber uint16
	DataType        pgtype.OID
	DataTypeSize    int16
	DataTypeName    string
	Modifier        int32
	FormatCode      int16
}

func (fd FieldDescription) Length() (int64, bool) {
	switch fd.DataType {
	case pgtype.TextOID, pgtype.ByteaOID:
		return math.MaxInt64, true
	case pgtype.VarcharOID, pgtype.BPCharArrayOID:
		return int64(fd.Modifier - varHeaderSize), true
	default:
		return 0, false
	}
}

func (fd FieldDescription) PrecisionScale() (precision, scale int64, ok bool) {
	switch fd.DataType {
	case pgtype.NumericOID:
		mod := fd.Modifier - varHeaderSize
		precision = int64((mod >> 16) & 0xffff)
		scale = int64(mod & 0xffff)
		return precision, scale, true
	default:
		return 0, 0, false
	}
}

func (fd FieldDescription) Type() reflect.Type {
	switch fd.DataType {
	case pgtype.Float8OID:
		return reflect.TypeOf(float64(0))
	case pgtype.Float4OID:
		return reflect.TypeOf(float32(0))
	case pgtype.Int8OID:
		return reflect.TypeOf(int64(0))
	case pgtype.Int4OID:
		return reflect.TypeOf(int32(0))
	case pgtype.Int2OID:
		return reflect.TypeOf(int16(0))
	case pgtype.VarcharOID, pgtype.BPCharArrayOID, pgtype.TextOID:
		return reflect.TypeOf("")
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.NumericOID:
		return reflect.TypeOf(float64(0))
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return reflect.TypeOf(time.Time{})
	case pgtype.ByteaOID:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}

func convertDriverValuers(args []interface{}) ([]interface{}, error) {
	for i, arg := range args {
		switch arg := arg.(type) {
		case pgtype.BinaryEncoder:
		case pgtype.TextEncoder:
		case driver.Valuer:
			v, err := callValuerValue(arg)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
	}
	return args, nil
}

// appendQuery appends a PostgreSQL wire protocol query message to buf and returns it.
func appendQuery(buf []byte, query string) []byte {
	buf = append(buf, 'Q')
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	buf = append(buf, query...)
	buf = append(buf, 0)
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	return buf
}
