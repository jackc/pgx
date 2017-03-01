package pgtype

import (
	"bytes"
	"io"

	"github.com/jackc/pgx/pgio"
)

type Int2Array struct {
	Elements   []Int2
	Dimensions []ArrayDimension
	Status     Status
}

func (a *Int2Array) ConvertFrom(src interface{}) error {
	return nil
}

func (a *Int2Array) AssignTo(dst interface{}) error {
	return nil
}

func (a *Int2Array) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*a = Int2Array{Status: Null}
		return nil
	}

	buf := make([]byte, int(size))
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	uta, err := ParseUntypedTextArray(string(buf))
	if err != nil {
		return err
	}

	textElementReader := NewTextElementReader(r)
	var elements []Int2

	if len(uta.Elements) > 0 {
		elements = make([]Int2, len(uta.Elements))

		for i, s := range uta.Elements {
			var elem Int2
			textElementReader.Reset(s)
			err = elem.DecodeText(textElementReader)
			if err != nil {
				return err
			}

			elements[i] = elem
		}
	}

	*a = Int2Array{Elements: elements, Dimensions: uta.Dimensions, Status: Present}

	return nil
}

func (a *Int2Array) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*a = Int2Array{Status: Null}
		return nil
	}

	var arrayHeader ArrayHeader
	err = arrayHeader.DecodeBinary(r)
	if err != nil {
		return err
	}

	if len(arrayHeader.Dimensions) == 0 {
		*a = Int2Array{Dimensions: arrayHeader.Dimensions, Status: Present}
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	elements := make([]Int2, elementCount)

	for i := range elements {
		err = elements[i].DecodeBinary(r)
		if err != nil {
			return err
		}
	}

	*a = Int2Array{Elements: elements, Dimensions: arrayHeader.Dimensions, Status: Present}
	return nil
}

func (a *Int2Array) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, a.Status); done {
		return err
	}

	if len(a.Dimensions) == 0 {
		_, err := pgio.WriteInt32(w, 2)
		if err != nil {
			return err
		}

		_, err = w.Write([]byte("{}"))
		return err
	}

	buf := &bytes.Buffer{}

	err := EncodeTextArrayDimensions(buf, a.Dimensions)
	if err != nil {
		return err
	}

	// dimElemCounts is the multiples of elements that each array lies on. For
	// example, a single dimension array of length 4 would have a dimElemCounts of
	// [4]. A multi-dimensional array of lengths [3,5,2] would have a
	// dimElemCounts of [30,10,2]. This is used to simplify when to render a '{'
	// or '}'.
	dimElemCounts := make([]int, len(a.Dimensions))
	dimElemCounts[len(a.Dimensions)-1] = int(a.Dimensions[len(a.Dimensions)-1].Length)
	for i := len(a.Dimensions) - 2; i > -1; i-- {
		dimElemCounts[i] = int(a.Dimensions[i].Length) * dimElemCounts[i+1]
	}

	textElementWriter := NewTextElementWriter(buf)

	for i, elem := range a.Elements {
		if i > 0 {
			err = pgio.WriteByte(buf, ',')
			if err != nil {
				return err
			}
		}

		for _, dec := range dimElemCounts {
			if i%dec == 0 {
				err = pgio.WriteByte(buf, '{')
				if err != nil {
					return err
				}
			}
		}

		textElementWriter.Reset()
		err = elem.EncodeText(textElementWriter)
		if err != nil {
			return err
		}

		for _, dec := range dimElemCounts {
			if (i+1)%dec == 0 {
				err = pgio.WriteByte(buf, '}')
				if err != nil {
					return err
				}
			}
		}
	}

	_, err = pgio.WriteInt32(w, int32(buf.Len()))
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(w)
	return err
}

func (a *Int2Array) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, a.Status); done {
		return err
	}

	var arrayHeader ArrayHeader

	// TODO - consider how to avoid having to buffer array before writing length -
	// or how not pay allocations for the byte order conversions.
	elemBuf := &bytes.Buffer{}

	for i := range a.Elements {
		err := a.Elements[i].EncodeBinary(elemBuf)
		if err != nil {
			return err
		}
		if a.Elements[i].Status == Null {
			arrayHeader.ContainsNull = true
		}
	}

	arrayHeader.ElementOID = Int2OID
	arrayHeader.Dimensions = a.Dimensions

	// TODO - consider how to avoid having to buffer array before writing length -
	// or how not pay allocations for the byte order conversions.
	headerBuf := &bytes.Buffer{}
	err := arrayHeader.EncodeBinary(headerBuf)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, int32(headerBuf.Len()+elemBuf.Len()))
	if err != nil {
		return err
	}

	_, err = headerBuf.WriteTo(w)
	if err != nil {
		return err
	}

	_, err = elemBuf.WriteTo(w)
	if err != nil {
		return err
	}

	return err
}
