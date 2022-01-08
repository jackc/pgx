package pgtype

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

type TextScanner interface {
	ScanText(v Text) error
}

type TextValuer interface {
	TextValue() (Text, error)
}

type Text struct {
	String string
	Valid  bool
}

func (t *Text) ScanText(v Text) error {
	*t = v
	return nil
}

func (t Text) TextValue() (Text, error) {
	return t, nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Text) Scan(src interface{}) error {
	if src == nil {
		*dst = Text{}
		return nil
	}

	switch src := src.(type) {
	case string:
		*dst = Text{String: src, Valid: true}
		return nil
	case []byte:
		*dst = Text{String: string(src), Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Text) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}
	return src.String, nil
}

func (src Text) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	return json.Marshal(src.String)
}

func (dst *Text) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*dst = Text{}
	} else {
		*dst = Text{String: *s, Valid: true}
	}

	return nil
}

type TextCodec struct{}

func (TextCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (TextCodec) PreferredFormat() int16 {
	return TextFormatCode
}

func (TextCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	switch format {
	case TextFormatCode, BinaryFormatCode:
		switch value.(type) {
		case string:
			return encodePlanTextCodecString{}
		case []byte:
			return encodePlanTextCodecByteSlice{}
		case rune:
			return encodePlanTextCodecRune{}
		case fmt.Stringer:
			return encodePlanTextCodecStringer{}
		case TextValuer:
			return encodePlanTextCodecTextValuer{}
		}
	}

	return nil
}

type encodePlanTextCodecString struct{}

func (encodePlanTextCodecString) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	s := value.(string)
	buf = append(buf, s...)
	return buf, nil
}

type encodePlanTextCodecByteSlice struct{}

func (encodePlanTextCodecByteSlice) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	s := value.([]byte)
	buf = append(buf, s...)
	return buf, nil
}

type encodePlanTextCodecRune struct{}

func (encodePlanTextCodecRune) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	r := value.(rune)
	buf = append(buf, string(r)...)
	return buf, nil
}

type encodePlanTextCodecStringer struct{}

func (encodePlanTextCodecStringer) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	s := value.(fmt.Stringer)
	buf = append(buf, s.String()...)
	return buf, nil
}

type encodePlanTextCodecTextValuer struct{}

func (encodePlanTextCodecTextValuer) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	text, err := value.(TextValuer).TextValue()
	if err != nil {
		return nil, err
	}

	if !text.Valid {
		return nil, nil
	}

	buf = append(buf, text.String...)
	return buf, nil
}

func (TextCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {

	switch format {
	case TextFormatCode, BinaryFormatCode:
		switch target.(type) {
		case *string:
			return scanPlanTextAnyToString{}
		case *[]byte:
			return scanPlanAnyToNewByteSlice{}
		case TextScanner:
			return scanPlanTextAnyToTextScanner{}
		case *rune:
			return scanPlanTextAnyToRune{}
		}
	}

	return nil
}

func (c TextCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.DecodeValue(ci, oid, format, src)
}

func (c TextCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	return string(src), nil
}

type scanPlanTextAnyToString struct{}

func (scanPlanTextAnyToString) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	p := (dst).(*string)
	*p = string(src)

	return nil
}

type scanPlanAnyToNewByteSlice struct{}

func (scanPlanAnyToNewByteSlice) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	p := (dst).(*[]byte)
	if src == nil {
		*p = nil
	} else {
		*p = make([]byte, len(src))
		copy(*p, src)
	}

	return nil
}

type scanPlanTextAnyToRune struct{}

func (scanPlanTextAnyToRune) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan null into %T", dst)
	}

	r, size := utf8.DecodeRune(src)
	if size != len(src) {
		return fmt.Errorf("cannot scan %v into %T: more than one rune received", src, dst)
	}

	p := (dst).(*rune)
	*p = r

	return nil
}

type scanPlanTextAnyToTextScanner struct{}

func (scanPlanTextAnyToTextScanner) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	scanner := (dst).(TextScanner)

	if src == nil {
		return scanner.ScanText(Text{})
	}

	return scanner.ScanText(Text{String: string(src), Valid: true})
}
