package pgtype

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/pgio"
)

type InetArray struct {
	Elements   []Inet
	Dimensions []ArrayDimension
	Status     Status
}

func (dst *InetArray) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case InetArray:
		*dst = value

	case []*net.IPNet:
		if value == nil {
			*dst = InetArray{Status: Null}
		} else if len(value) == 0 {
			*dst = InetArray{Status: Present}
		} else {
			elements := make([]Inet, len(value))
			for i := range value {
				if err := elements[i].ConvertFrom(value[i]); err != nil {
					return err
				}
			}
			*dst = InetArray{
				Elements:   elements,
				Dimensions: []ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
				Status:     Present,
			}
		}

	case []net.IP:
		if value == nil {
			*dst = InetArray{Status: Null}
		} else if len(value) == 0 {
			*dst = InetArray{Status: Present}
		} else {
			elements := make([]Inet, len(value))
			for i := range value {
				if err := elements[i].ConvertFrom(value[i]); err != nil {
					return err
				}
			}
			*dst = InetArray{
				Elements:   elements,
				Dimensions: []ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
				Status:     Present,
			}
		}

	default:
		if originalSrc, ok := underlyingSliceType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Inet", value)
	}

	return nil
}

func (src *InetArray) AssignTo(dst interface{}) error {
	switch v := dst.(type) {

	case *[]*net.IPNet:
		if src.Status == Present {
			*v = make([]*net.IPNet, len(src.Elements))
			for i := range src.Elements {
				if err := src.Elements[i].AssignTo(&((*v)[i])); err != nil {
					return err
				}
			}
		} else {
			*v = nil
		}

	case *[]net.IP:
		if src.Status == Present {
			*v = make([]net.IP, len(src.Elements))
			for i := range src.Elements {
				if err := src.Elements[i].AssignTo(&((*v)[i])); err != nil {
					return err
				}
			}
		} else {
			*v = nil
		}

	default:
		if originalDst, ok := underlyingPtrSliceType(dst); ok {
			return src.AssignTo(originalDst)
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *InetArray) DecodeText(src []byte) error {
	if src == nil {
		*dst = InetArray{Status: Null}
		return nil
	}

	uta, err := ParseUntypedTextArray(string(src))
	if err != nil {
		return err
	}

	var elements []Inet

	if len(uta.Elements) > 0 {
		elements = make([]Inet, len(uta.Elements))

		for i, s := range uta.Elements {
			var elem Inet
			var elemSrc []byte
			if s != "NULL" {
				elemSrc = []byte(s)
			}
			err = elem.DecodeText(elemSrc)
			if err != nil {
				return err
			}

			elements[i] = elem
		}
	}

	*dst = InetArray{Elements: elements, Dimensions: uta.Dimensions, Status: Present}

	return nil
}

func (dst *InetArray) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = InetArray{Status: Null}
		return nil
	}

	var arrayHeader ArrayHeader
	rp, err := arrayHeader.DecodeBinary(src)
	if err != nil {
		return err
	}

	if len(arrayHeader.Dimensions) == 0 {
		*dst = InetArray{Dimensions: arrayHeader.Dimensions, Status: Present}
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	elements := make([]Inet, elementCount)

	for i := range elements {
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}
		err = elements[i].DecodeBinary(elemSrc)
		if err != nil {
			return err
		}
	}

	*dst = InetArray{Elements: elements, Dimensions: arrayHeader.Dimensions, Status: Present}
	return nil
}

func (src *InetArray) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	if len(src.Dimensions) == 0 {
		_, err := io.WriteString(w, "{}")
		return false, err
	}

	err := EncodeTextArrayDimensions(w, src.Dimensions)
	if err != nil {
		return false, err
	}

	// dimElemCounts is the multiples of elements that each array lies on. For
	// example, a single dimension array of length 4 would have a dimElemCounts of
	// [4]. A multi-dimensional array of lengths [3,5,2] would have a
	// dimElemCounts of [30,10,2]. This is used to simplify when to render a '{'
	// or '}'.
	dimElemCounts := make([]int, len(src.Dimensions))
	dimElemCounts[len(src.Dimensions)-1] = int(src.Dimensions[len(src.Dimensions)-1].Length)
	for i := len(src.Dimensions) - 2; i > -1; i-- {
		dimElemCounts[i] = int(src.Dimensions[i].Length) * dimElemCounts[i+1]
	}

	for i, elem := range src.Elements {
		if i > 0 {
			err = pgio.WriteByte(w, ',')
			if err != nil {
				return false, err
			}
		}

		for _, dec := range dimElemCounts {
			if i%dec == 0 {
				err = pgio.WriteByte(w, '{')
				if err != nil {
					return false, err
				}
			}
		}

		elemBuf := &bytes.Buffer{}
		null, err := elem.EncodeText(elemBuf)
		if err != nil {
			return false, err
		}
		if null {
			_, err = io.WriteString(w, `NULL`)
			if err != nil {
				return false, err
			}
		} else {
			_, err = io.WriteString(w, QuoteArrayElementIfNeeded(elemBuf.String()))
			if err != nil {
				return false, err
			}
		}

		for _, dec := range dimElemCounts {
			if (i+1)%dec == 0 {
				err = pgio.WriteByte(w, '}')
				if err != nil {
					return false, err
				}
			}
		}
	}

	return false, nil
}

func (src *InetArray) EncodeBinary(w io.Writer) (bool, error) {
	return src.encodeBinary(w, InetOID)
}

func (src *InetArray) encodeBinary(w io.Writer, elementOID int32) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	arrayHeader := ArrayHeader{
		ElementOID: elementOID,
		Dimensions: src.Dimensions,
	}

	for i := range src.Elements {
		if src.Elements[i].Status == Null {
			arrayHeader.ContainsNull = true
			break
		}
	}

	err := arrayHeader.EncodeBinary(w)
	if err != nil {
		return false, err
	}

	elemBuf := &bytes.Buffer{}

	for i := range src.Elements {
		elemBuf.Reset()

		null, err := src.Elements[i].EncodeBinary(elemBuf)
		if err != nil {
			return false, err
		}
		if null {
			_, err = pgio.WriteInt32(w, -1)
			if err != nil {
				return false, err
			}
		} else {
			_, err = pgio.WriteInt32(w, int32(elemBuf.Len()))
			if err != nil {
				return false, err
			}
			_, err = elemBuf.WriteTo(w)
			if err != nil {
				return false, err
			}
		}
	}

	return false, err
}
