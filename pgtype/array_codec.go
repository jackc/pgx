package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/jackc/pgio"
)

// ArrayGetter is a type that can be converted into a PostgreSQL array.
type ArrayGetter interface {
	// Dimensions returns the array dimensions. If array is nil then nil is returned.
	Dimensions() []ArrayDimension

	// Index returns the element at i.
	Index(i int) interface{}

	// IndexType returns a non-nil scan target of the type Index will return. This is used by ArrayCodec.PlanEncode.
	IndexType() interface{}
}

// ArraySetter is a type can be set from a PostgreSQL array.
type ArraySetter interface {
	// SetDimensions prepares the value such that ScanIndex can be called for each element. dimensions may be nil to
	// indicate a NULL array. If unable to exactly preserve dimensions SetDimensions may return an error or silently
	// flatten the array dimensions.
	SetDimensions(dimensions []ArrayDimension) error

	// ScanIndex returns a value usable as a scan target for i. SetDimensions must be called before ScanIndex.
	ScanIndex(i int) interface{}

	// ScanIndexType returns a non-nil scan target of the type ScanIndex will return. This is used by
	// ArrayCodec.PlanScan.
	ScanIndexType() interface{}
}

// ArrayCodec is a codec for any array type.
type ArrayCodec struct {
	ElementDataType *DataType
}

func (c *ArrayCodec) FormatSupported(format int16) bool {
	return c.ElementDataType.Codec.FormatSupported(format)
}

func (c *ArrayCodec) PreferredFormat() int16 {
	return c.ElementDataType.Codec.PreferredFormat()
}

func (c *ArrayCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	arrayValuer, ok := value.(ArrayGetter)
	if !ok {
		return nil
	}

	elementType := arrayValuer.IndexType()

	elementEncodePlan := ci.PlanEncode(c.ElementDataType.OID, format, elementType)
	if elementEncodePlan == nil {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return &encodePlanArrayCodecBinary{ac: c, ci: ci, oid: oid}
	case TextFormatCode:
		return &encodePlanArrayCodecText{ac: c, ci: ci, oid: oid}
	}

	return nil
}

type encodePlanArrayCodecText struct {
	ac  *ArrayCodec
	ci  *ConnInfo
	oid uint32
}

func (p *encodePlanArrayCodecText) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	array := value.(ArrayGetter)

	dimensions := array.Dimensions()
	if dimensions == nil {
		return nil, nil
	}

	elementCount := cardinality(dimensions)
	if elementCount == 0 {
		return append(buf, '{', '}'), nil
	}

	buf = EncodeTextArrayDimensions(buf, dimensions)

	// dimElemCounts is the multiples of elements that each array lies on. For
	// example, a single dimension array of length 4 would have a dimElemCounts of
	// [4]. A multi-dimensional array of lengths [3,5,2] would have a
	// dimElemCounts of [30,10,2]. This is used to simplify when to render a '{'
	// or '}'.
	dimElemCounts := make([]int, len(dimensions))
	dimElemCounts[len(dimensions)-1] = int(dimensions[len(dimensions)-1].Length)
	for i := len(dimensions) - 2; i > -1; i-- {
		dimElemCounts[i] = int(dimensions[i].Length) * dimElemCounts[i+1]
	}

	var encodePlan EncodePlan
	var lastElemType reflect.Type
	inElemBuf := make([]byte, 0, 32)
	for i := 0; i < elementCount; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}

		for _, dec := range dimElemCounts {
			if i%dec == 0 {
				buf = append(buf, '{')
			}
		}

		elem := array.Index(i)
		var elemBuf []byte
		if elem != nil {
			elemType := reflect.TypeOf(elem)
			if lastElemType != elemType {
				lastElemType = elemType
				encodePlan = p.ci.PlanEncode(p.ac.ElementDataType.OID, TextFormatCode, elem)
				if encodePlan == nil {
					return nil, fmt.Errorf("unable to encode %v", array.Index(i))
				}
			}
			elemBuf, err = encodePlan.Encode(elem, inElemBuf)
			if err != nil {
				return nil, err
			}
		}

		if elemBuf == nil {
			buf = append(buf, `NULL`...)
		} else {
			buf = append(buf, QuoteArrayElementIfNeeded(string(elemBuf))...)
		}

		for _, dec := range dimElemCounts {
			if (i+1)%dec == 0 {
				buf = append(buf, '}')
			}
		}
	}

	return buf, nil
}

type encodePlanArrayCodecBinary struct {
	ac  *ArrayCodec
	ci  *ConnInfo
	oid uint32
}

func (p *encodePlanArrayCodecBinary) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	array := value.(ArrayGetter)

	dimensions := array.Dimensions()
	if dimensions == nil {
		return nil, nil
	}

	arrayHeader := ArrayHeader{
		Dimensions: dimensions,
		ElementOID: p.ac.ElementDataType.OID,
	}

	containsNullIndex := len(buf) + 4

	buf = arrayHeader.EncodeBinary(buf)

	elementCount := cardinality(dimensions)

	var encodePlan EncodePlan
	var lastElemType reflect.Type
	for i := 0; i < elementCount; i++ {
		sp := len(buf)
		buf = pgio.AppendInt32(buf, -1)

		elem := array.Index(i)
		var elemBuf []byte
		if elem != nil {
			elemType := reflect.TypeOf(elem)
			if lastElemType != elemType {
				lastElemType = elemType
				encodePlan = p.ci.PlanEncode(p.ac.ElementDataType.OID, BinaryFormatCode, elem)
				if encodePlan == nil {
					return nil, fmt.Errorf("unable to encode %v", array.Index(i))
				}
			}
			elemBuf, err = encodePlan.Encode(elem, buf)
			if err != nil {
				return nil, err
			}
		}

		if elemBuf == nil {
			pgio.SetInt32(buf[containsNullIndex:], 1)
		} else {
			buf = elemBuf
			pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
		}
	}

	return buf, nil
}

func (c *ArrayCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {
	arrayScanner, ok := target.(ArraySetter)
	if !ok {
		return nil
	}

	elementType := arrayScanner.ScanIndexType()

	elementScanPlan := ci.PlanScan(c.ElementDataType.OID, format, elementType)
	if _, ok := elementScanPlan.(*scanPlanFail); ok {
		return nil
	}

	return &scanPlanArrayCodec{
		arrayCodec: c,
		ci:         ci,
		oid:        oid,
		formatCode: format,
	}
}

func (c *ArrayCodec) decodeBinary(ci *ConnInfo, arrayOID uint32, src []byte, array ArraySetter) error {
	var arrayHeader ArrayHeader
	rp, err := arrayHeader.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	err = array.SetDimensions(arrayHeader.Dimensions)
	if err != nil {
		return err
	}

	elementCount := cardinality(arrayHeader.Dimensions)
	if elementCount == 0 {
		return nil
	}

	elementScanPlan := c.ElementDataType.Codec.PlanScan(ci, c.ElementDataType.OID, BinaryFormatCode, array.ScanIndex(0), false)
	if elementScanPlan == nil {
		elementScanPlan = ci.PlanScan(c.ElementDataType.OID, BinaryFormatCode, array.ScanIndex(0))
	}

	for i := 0; i < elementCount; i++ {
		elem := array.ScanIndex(i)
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}
		err = elementScanPlan.Scan(elemSrc, elem)
		if err != nil {
			return fmt.Errorf("failed to scan array element %d: %w", i, err)
		}
	}

	return nil
}

func (c *ArrayCodec) decodeText(ci *ConnInfo, arrayOID uint32, src []byte, array ArraySetter) error {
	uta, err := ParseUntypedTextArray(string(src))
	if err != nil {
		return err
	}

	err = array.SetDimensions(uta.Dimensions)
	if err != nil {
		return err
	}

	if len(uta.Elements) == 0 {
		return nil
	}

	elementScanPlan := c.ElementDataType.Codec.PlanScan(ci, c.ElementDataType.OID, TextFormatCode, array.ScanIndex(0), false)
	if elementScanPlan == nil {
		elementScanPlan = ci.PlanScan(c.ElementDataType.OID, TextFormatCode, array.ScanIndex(0))
	}

	for i, s := range uta.Elements {
		elem := array.ScanIndex(i)
		var elemSrc []byte
		if s != "NULL" {
			elemSrc = []byte(s)
		}

		err = elementScanPlan.Scan(elemSrc, elem)
		if err != nil {
			return err
		}
	}

	return nil
}

type scanPlanArrayCodec struct {
	arrayCodec      *ArrayCodec
	ci              *ConnInfo
	oid             uint32
	formatCode      int16
	elementScanPlan ScanPlan
}

func (spac *scanPlanArrayCodec) Scan(src []byte, dst interface{}) error {
	c := spac.arrayCodec
	ci := spac.ci
	oid := spac.oid
	formatCode := spac.formatCode

	array := dst.(ArraySetter)

	if src == nil {
		return array.SetDimensions(nil)
	}

	switch formatCode {
	case BinaryFormatCode:
		return c.decodeBinary(ci, oid, src, array)
	case TextFormatCode:
		return c.decodeText(ci, oid, src, array)
	default:
		return fmt.Errorf("unknown format code %d", formatCode)
	}
}

func (c *ArrayCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	switch format {
	case TextFormatCode:
		return string(src), nil
	case BinaryFormatCode:
		buf := make([]byte, len(src))
		copy(buf, src)
		return buf, nil
	default:
		return nil, fmt.Errorf("unknown format code %d", format)
	}
}

func (c *ArrayCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var slice []interface{}
	err := ci.PlanScan(oid, format, &slice).Scan(src, &slice)
	return slice, err
}

func isRagged(slice reflect.Value) bool {
	if slice.Type().Elem().Kind() != reflect.Slice {
		return false
	}

	sliceLen := slice.Len()
	innerLen := 0
	for i := 0; i < sliceLen; i++ {
		if i == 0 {
			innerLen = slice.Index(i).Len()
		} else {
			if slice.Index(i).Len() != innerLen {
				return true
			}
		}
		if isRagged(slice.Index(i)) {
			return true
		}
	}

	return false
}
