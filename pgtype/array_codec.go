package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"

	"github.com/jackc/pgio"
)

// ArrayGetter is a type that can be converted into a PostgreSQL array.
type ArrayGetter interface {
	// Dimensions returns the array dimensions. If array is nil then nil is returned.
	Dimensions() []ArrayDimension

	// Index returns the element at i.
	Index(i int) interface{}
}

// ArraySetter is a type can be set from a PostgreSQL array.
type ArraySetter interface {
	// SetDimensions prepares the value such that ScanIndex can be called for each element. dimensions may be nil to
	// indicate a NULL array. If unable to exactly preserve dimensions SetDimensions may return an error or silently
	// flatten the array dimensions.
	SetDimensions(dimensions []ArrayDimension) error

	// ScanIndex returns a value usable as a scan target for i. SetDimensions must be called before ScanIndex.
	ScanIndex(i int) interface{}
}

// ArrayCodec is a codec for any array type.
type ArrayCodec struct {
	ElementCodec Codec
	ElementOID   uint32
}

func (c *ArrayCodec) FormatSupported(format int16) bool {
	return c.ElementCodec.FormatSupported(format)
}

func (c *ArrayCodec) PreferredFormat() int16 {
	return c.ElementCodec.PreferredFormat()
}

func (c *ArrayCodec) Encode(ci *ConnInfo, oid uint32, format int16, value interface{}, buf []byte) (newBuf []byte, err error) {
	if value == nil {
		return nil, nil
	}

	array, err := makeArrayGetter(value)
	if err != nil {
		return nil, err
	}

	switch format {
	case BinaryFormatCode:
		return c.encodeBinary(ci, oid, array, buf)
	case TextFormatCode:
		return c.encodeText(ci, oid, array, buf)
	default:
		return nil, fmt.Errorf("unknown format code: %v", format)
	}

}

func (c *ArrayCodec) encodeBinary(ci *ConnInfo, oid uint32, array ArrayGetter, buf []byte) (newBuf []byte, err error) {
	dimensions := array.Dimensions()
	if dimensions == nil {
		return nil, nil
	}

	arrayHeader := ArrayHeader{
		Dimensions: dimensions,
		ElementOID: int32(c.ElementOID),
	}

	containsNullIndex := len(buf) + 4

	buf = arrayHeader.EncodeBinary(ci, buf)

	elementCount := cardinality(dimensions)
	for i := 0; i < elementCount; i++ {
		sp := len(buf)
		buf = pgio.AppendInt32(buf, -1)

		elemBuf, err := c.ElementCodec.Encode(ci, c.ElementOID, BinaryFormatCode, array.Index(i), buf)
		if err != nil {
			return nil, err
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

func (c *ArrayCodec) encodeText(ci *ConnInfo, oid uint32, array ArrayGetter, buf []byte) (newBuf []byte, err error) {
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

		elemBuf, err := c.ElementCodec.Encode(ci, c.ElementOID, TextFormatCode, array.Index(i), inElemBuf)
		if err != nil {
			return nil, err
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

func (c *ArrayCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {
	_, err := makeArraySetter(target)
	if err != nil {
		return nil
	}

	return (*scanPlanArrayCodec)(c)
}

func (c *ArrayCodec) decodeBinary(ci *ConnInfo, arrayOID uint32, src []byte, array ArraySetter) error {
	var arrayHeader ArrayHeader
	rp, err := arrayHeader.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	// TODO - ArrayHeader.DecodeBinary should do this. But doing this there breaks old array code. Leave until old code
	// can be removed.
	if arrayHeader.Dimensions == nil {
		arrayHeader.Dimensions = []ArrayDimension{}
	}

	err = array.SetDimensions(arrayHeader.Dimensions)
	if err != nil {
		return err
	}

	elementCount := cardinality(arrayHeader.Dimensions)
	if elementCount == 0 {
		return nil
	}

	elementScanPlan := c.ElementCodec.PlanScan(ci, c.ElementOID, BinaryFormatCode, array.ScanIndex(0), false)
	if elementScanPlan == nil {
		elementScanPlan = ci.PlanScan(c.ElementOID, BinaryFormatCode, array.ScanIndex(0))
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
		err = elementScanPlan.Scan(ci, c.ElementOID, BinaryFormatCode, elemSrc, elem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ArrayCodec) decodeText(ci *ConnInfo, arrayOID uint32, src []byte, array ArraySetter) error {
	uta, err := ParseUntypedTextArray(string(src))
	if err != nil {
		return err
	}

	// TODO - ParseUntypedTextArray should do this. But doing this there breaks old array code. Leave until old code
	// can be removed.
	if uta.Dimensions == nil {
		uta.Dimensions = []ArrayDimension{}
	}

	err = array.SetDimensions(uta.Dimensions)
	if err != nil {
		return err
	}

	if len(uta.Elements) == 0 {
		return nil
	}

	elementScanPlan := c.ElementCodec.PlanScan(ci, c.ElementOID, TextFormatCode, array.ScanIndex(0), false)
	if elementScanPlan == nil {
		elementScanPlan = ci.PlanScan(c.ElementOID, TextFormatCode, array.ScanIndex(0))
	}

	for i, s := range uta.Elements {
		elem := array.ScanIndex(i)
		var elemSrc []byte
		if s != "NULL" {
			elemSrc = []byte(s)
		}

		err = elementScanPlan.Scan(ci, c.ElementOID, TextFormatCode, elemSrc, elem)
		if err != nil {
			return err
		}
	}

	return nil
}

type scanPlanArrayCodec ArrayCodec

func (spac *scanPlanArrayCodec) Scan(ci *ConnInfo, oid uint32, formatCode int16, src []byte, dst interface{}) error {
	c := (*ArrayCodec)(spac)

	array, err := makeArraySetter(dst)
	if err != nil {
		newPlan := ci.PlanScan(oid, formatCode, dst)
		return newPlan.Scan(ci, oid, formatCode, src, dst)
	}

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

func (c ArrayCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	// var n int64
	// err := c.PlanScan(ci, oid, format, &n, true).Scan(ci, oid, format, src, &n)
	// return n, err

	return nil, fmt.Errorf("not implemented")
}

func (c ArrayCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	// var n int16
	// err := c.PlanScan(ci, oid, format, &n, true).Scan(ci, oid, format, src, &n)
	// return n, err

	return nil, fmt.Errorf("not implemented")
}
