package pgtype

import (
	"bytes"
	"errors"
)

func DatabaseSQLValue(ci *ConnInfo, src Value) (interface{}, error) {
	switch src := src.(type) {
	case *Bool:
		return src.Bool, nil
	case *Bytea:
		return src.Bytes, nil
	case *Date:
		if src.InfinityModifier == None {
			return src.Time, nil
		}
	case *Float4:
		return float64(src.Float), nil
	case *Float8:
		return src.Float, nil
	case *GenericBinary:
		return src.Bytes, nil
	case *GenericText:
		return src.String, nil
	case *Int2:
		return int64(src.Int), nil
	case *Int4:
		return int64(src.Int), nil
	case *Int8:
		return int64(src.Int), nil
	case *Text:
		return src.String, nil
	case *Timestamp:
		if src.InfinityModifier == None {
			return src.Time, nil
		}
	case *Timestamptz:
		if src.InfinityModifier == None {
			return src.Time, nil
		}
	case *Unknown:
		return src.String, nil
	case *Varchar:
		return src.String, nil
	}

	buf := &bytes.Buffer{}
	if textEncoder, ok := src.(TextEncoder); ok {
		_, err := textEncoder.EncodeText(ci, buf)
		if err != nil {
			return nil, err
		}
		return buf.String(), nil
	}

	if binaryEncoder, ok := src.(BinaryEncoder); ok {
		_, err := binaryEncoder.EncodeBinary(ci, buf)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	return nil, errors.New("cannot convert to database/sql compatible value")
}
