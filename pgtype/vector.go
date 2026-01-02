package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/internal/pgio"
)

type VectorScanner interface {
	ScanVector(v Vector) error
}

type VectorValuer interface {
	VectorValue() (Vector, error)
}

type Vector struct {
	Vec   []float32
	Valid bool
}

// ScanVector implements the [VectorScanner] interface.
func (v *Vector) ScanVector(val Vector) error {
	*v = val
	return nil
}

// VectorValue implements the [VectorValuer] interface.
func (v Vector) VectorValue() (Vector, error) {
	return v, nil
}

// Scan implements the [database/sql.Scanner] interface.
func (dst *Vector) Scan(src any) error {
	if src == nil {
		*dst = Vector{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextAnyToVectorScanner{}.Scan([]byte(src), dst)
	case []byte:
		return scanPlanTextAnyToVectorScanner{}.Scan(src, dst)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the [database/sql/driver.Valuer] interface.
func (src Vector) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	buf, err := VectorCodec{}.PlanEncode(nil, 0, TextFormatCode, src).Encode(src, nil)
	if err != nil {
		return nil, err
	}
	return string(buf), err
}

// MarshalJSON implements the [encoding/json.Marshaler] interface.
func (src Vector) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}

	return []byte(src.String()), nil
}

// UnmarshalJSON implements the [encoding/json.Unmarshaler] interface.
func (dst *Vector) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*dst = Vector{}
		return nil
	}

	vec, err := parseVector(string(b))
	if err != nil {
		return err
	}
	*dst = vec
	return nil
}

func (v Vector) String() string {
	if !v.Valid {
		return ""
	}

	var b strings.Builder
	b.WriteString("[")
	for i, val := range v.Vec {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(strconv.FormatFloat(float64(val), 'g', -1, 32))
	}
	b.WriteString("]")
	return b.String()
}

func parseVector(s string) (Vector, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return Vector{}, fmt.Errorf("invalid vector format")
	}

	s = s[1 : len(s)-1]
	if s == "" {
		return Vector{Vec: []float32{}, Valid: true}, nil
	}

	parts := strings.Split(s, ",")
	vec := make([]float32, len(parts))
	for i, part := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return Vector{}, err
		}
		vec[i] = float32(f)
	}

	return Vector{Vec: vec, Valid: true}, nil
}

type VectorCodec struct{}

func (VectorCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (VectorCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (VectorCodec) PlanEncode(m *Map, oid uint32, format int16, value any) EncodePlan {
	if _, ok := value.(VectorValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanVectorCodecBinary{}
	case TextFormatCode:
		return encodePlanVectorCodecText{}
	}

	return nil
}

type encodePlanVectorCodecBinary struct{}

func (encodePlanVectorCodecBinary) Encode(value any, buf []byte) (newBuf []byte, err error) {
	vector, err := value.(VectorValuer).VectorValue()
	if err != nil {
		return nil, err
	}

	if !vector.Valid {
		return nil, nil
	}

	dim := uint16(len(vector.Vec))
	buf = pgio.AppendUint16(buf, dim)
	buf = pgio.AppendUint16(buf, 0)
	for _, v := range vector.Vec {
		buf = pgio.AppendUint32(buf, math.Float32bits(v))
	}
	return buf, nil
}

type encodePlanVectorCodecText struct{}

func (encodePlanVectorCodecText) Encode(value any, buf []byte) (newBuf []byte, err error) {
	vector, err := value.(VectorValuer).VectorValue()
	if err != nil {
		return nil, err
	}

	if !vector.Valid {
		return nil, nil
	}

	return append(buf, vector.String()...), nil
}

func (VectorCodec) PlanScan(m *Map, oid uint32, format int16, target any) ScanPlan {
	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case VectorScanner:
			return scanPlanBinaryVectorToVectorScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case VectorScanner:
			return scanPlanTextAnyToVectorScanner{}
		}
	}

	return nil
}

func (c VectorCodec) DecodeDatabaseSQLValue(m *Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return codecDecodeToTextFormat(c, m, oid, format, src)
}

func (c VectorCodec) DecodeValue(m *Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var vector Vector
	err := codecScan(c, m, oid, format, src, &vector)
	if err != nil {
		return nil, err
	}
	return vector, nil
}

type scanPlanBinaryVectorToVectorScanner struct{}

func (scanPlanBinaryVectorToVectorScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(VectorScanner)

	if src == nil {
		return scanner.ScanVector(Vector{})
	}

	if len(src) < 4 {
		return fmt.Errorf("invalid length for vector: %v", len(src))
	}

	dim := binary.BigEndian.Uint16(src)
	expectedLen := 4 + int(dim)*4
	if len(src) != expectedLen {
		return fmt.Errorf("invalid length for vector: expected %d, got %d", expectedLen, len(src))
	}

	vec := make([]float32, dim)
	for i := 0; i < int(dim); i++ {
		bits := binary.BigEndian.Uint32(src[4+i*4:])
		vec[i] = math.Float32frombits(bits)
	}

	return scanner.ScanVector(Vector{Vec: vec, Valid: true})
}

type scanPlanTextAnyToVectorScanner struct{}

func (scanPlanTextAnyToVectorScanner) Scan(src []byte, dst any) error {
	scanner := (dst).(VectorScanner)

	if src == nil {
		return scanner.ScanVector(Vector{})
	}

	vector, err := parseVector(string(src))
	if err != nil {
		return err
	}

	return scanner.ScanVector(vector)
}
