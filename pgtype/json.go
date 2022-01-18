package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
)

type JSONCodec struct{}

func (JSONCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (JSONCodec) PreferredFormat() int16 {
	return TextFormatCode
}

func (JSONCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	switch value.(type) {
	case []byte:
		return encodePlanJSONCodecEitherFormatByteSlice{}
	default:
		return encodePlanJSONCodecEitherFormatMarshal{}
	}
}

type encodePlanJSONCodecEitherFormatByteSlice struct{}

func (encodePlanJSONCodecEitherFormatByteSlice) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	jsonBytes := value.([]byte)
	if jsonBytes == nil {
		return nil, nil
	}

	buf = append(buf, jsonBytes...)
	return buf, nil
}

type encodePlanJSONCodecEitherFormatMarshal struct{}

func (encodePlanJSONCodecEitherFormatMarshal) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	buf = append(buf, jsonBytes...)
	return buf, nil
}

func (JSONCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {
	switch target.(type) {
	case *string:
		return scanPlanAnyToString{}
	case *[]byte:
		return scanPlanJSONToByteSlice{}
	case BytesScanner:
		return scanPlanBinaryBytesToBytesScanner{}
	default:
		return scanPlanJSONToJSONUnmarshal{}
	}

}

type scanPlanAnyToString struct{}

func (scanPlanAnyToString) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	p := dst.(*string)
	*p = string(src)
	return nil
}

type scanPlanJSONToByteSlice struct{}

func (scanPlanJSONToByteSlice) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	dstBuf := dst.(*[]byte)
	if src == nil {
		*dstBuf = nil
		return nil
	}

	*dstBuf = make([]byte, len(src))
	copy(*dstBuf, src)
	return nil
}

type scanPlanJSONToBytesScanner struct{}

func (scanPlanJSONToBytesScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	scanner := (dst).(BytesScanner)
	return scanner.ScanBytes(src)
}

type scanPlanJSONToJSONUnmarshal struct{}

func (scanPlanJSONToJSONUnmarshal) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		dstValue := reflect.ValueOf(dst)
		if dstValue.Kind() == reflect.Ptr {
			el := dstValue.Elem()
			switch el.Kind() {
			case reflect.Ptr, reflect.Slice, reflect.Map:
				el.Set(reflect.Zero(el.Type()))
				return nil
			}
		}
	}

	return json.Unmarshal(src, dst)
}

func (c JSONCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	dstBuf := make([]byte, len(src))
	copy(dstBuf, src)
	return dstBuf, nil
}

func (c JSONCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var dst interface{}
	err := json.Unmarshal(src, &dst)
	return dst, err
}
