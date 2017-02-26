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

	return nil
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

	// TODO - don't use magic number. Types with fixed OIDs should be constants.
	arrayHeader.ElementOID = 21
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
