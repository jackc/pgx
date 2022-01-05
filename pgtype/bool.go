package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
)

type BoolScanner interface {
	ScanBool(v bool, valid bool) error
}

type Bool struct {
	Bool  bool
	Valid bool
}

// ScanBool implements the BoolScanner interface.
func (dst *Bool) ScanBool(v bool, valid bool) error {
	if !valid {
		*dst = Bool{}
		return nil
	}

	*dst = Bool{Bool: v, Valid: true}

	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Bool) Scan(src interface{}) error {
	if src == nil {
		*dst = Bool{}
		return nil
	}

	switch src := src.(type) {
	case bool:
		*dst = Bool{Bool: src, Valid: true}
		return nil
	case string:
		b, err := strconv.ParseBool(src)
		if err != nil {
			return err
		}
		*dst = Bool{Bool: b, Valid: true}
		return nil
	case []byte:
		b, err := strconv.ParseBool(string(src))
		if err != nil {
			return err
		}
		*dst = Bool{Bool: b, Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Bool) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	return src.Bool, nil
}

func (src Bool) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	if src.Bool {
		return []byte("true"), nil
	} else {
		return []byte("false"), nil
	}
}

func (dst *Bool) UnmarshalJSON(b []byte) error {
	var v *bool
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}

	if v == nil {
		*dst = Bool{}
	} else {
		*dst = Bool{Bool: *v, Valid: true}
	}

	return nil
}

type BoolCodec struct{}

func (BoolCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (BoolCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (BoolCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	switch format {
	case BinaryFormatCode:
		return &encodePlanBoolCodecBinary{}
	case TextFormatCode:
		return &encodePlanBoolCodecText{}
	}

	return nil
}

type encodePlanBoolCodecBinary struct{}

func (p *encodePlanBoolCodecBinary) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	v, valid, err := convertToBoolForEncode(value)
	if err != nil {
		return nil, fmt.Errorf("cannot convert %v to bool: %v", value, err)
	}
	if !valid {
		return nil, nil
	}
	if value == nil {
		return nil, nil
	}

	if v {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	return buf, nil
}

type encodePlanBoolCodecText struct{}

func (p *encodePlanBoolCodecText) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	v, valid, err := convertToBoolForEncode(value)
	if err != nil {
		return nil, fmt.Errorf("cannot convert %v to bool: %v", value, err)
	}
	if !valid {
		return nil, nil
	}
	if value == nil {
		return nil, nil
	}

	if v {
		buf = append(buf, 't')
	} else {
		buf = append(buf, 'f')
	}

	return buf, nil
}

func (BoolCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {

	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case *bool:
			return scanPlanBinaryBoolToBool{}
		case BoolScanner:
			return scanPlanBinaryBoolToBoolScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case *bool:
			return scanPlanTextAnyToBool{}
		case BoolScanner:
			return scanPlanTextAnyToBoolScanner{}
		}
	}

	return nil
}

func (c BoolCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.DecodeValue(ci, oid, format, src)
}

func (c BoolCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var b bool
	err := codecScan(c, ci, oid, format, src, &b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func convertToBoolForEncode(v interface{}) (b bool, valid bool, err error) {
	if v == nil {
		return false, false, nil
	}

	switch v := v.(type) {
	case bool:
		return v, true, nil
	case *bool:
		if v == nil {
			return false, false, nil
		}
		return *v, true, nil
	case string:
		bb, err := strconv.ParseBool(v)
		if err != nil {
			return false, false, err
		}
		return bb, true, nil
	case *string:
		if v == nil {
			return false, false, nil
		}
		bb, err := strconv.ParseBool(*v)
		if err != nil {
			return false, false, err
		}
		return bb, true, nil
	default:
		if originalvalue, ok := underlyingBoolType(v); ok {
			return convertToBoolForEncode(originalvalue)
		}
		return false, false, fmt.Errorf("cannot convert %v to bool", v)
	}
}

type scanPlanBinaryBoolToBool struct{}

func (scanPlanBinaryBoolToBool) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	p, ok := (dst).(*bool)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	*p = src[0] == 1

	return nil
}

type scanPlanTextAnyToBool struct{}

func (scanPlanTextAnyToBool) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	p, ok := (dst).(*bool)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	*p = src[0] == 't'

	return nil
}

type scanPlanBinaryBoolToBoolScanner struct{}

func (scanPlanBinaryBoolToBoolScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	s, ok := (dst).(BoolScanner)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	if src == nil {
		return s.ScanBool(false, false)
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	return s.ScanBool(src[0] == 1, true)
}

type scanPlanTextAnyToBoolScanner struct{}

func (scanPlanTextAnyToBoolScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	s, ok := (dst).(BoolScanner)
	if !ok {
		return ErrScanTargetTypeChanged
	}

	if src == nil {
		return s.ScanBool(false, false)
	}

	if len(src) != 1 {
		return fmt.Errorf("invalid length for bool: %v", len(src))
	}

	return s.ScanBool(src[0] == 't', true)
}
