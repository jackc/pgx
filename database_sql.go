package pgtype

import (
	"bytes"
	"database/sql/driver"
	"errors"
)

func DatabaseSQLValue(ci *ConnInfo, src Value) (interface{}, error) {
	if valuer, ok := src.(driver.Valuer); ok {
		return valuer.Value()
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

func encodeValueText(src TextEncoder) (interface{}, error) {
	buf := &bytes.Buffer{}
	null, err := src.EncodeText(nil, buf)
	if err != nil {
		return nil, err
	}
	if null {
		return nil, nil
	}
	return buf.String(), err
}
