package pgtype

import (
	"bytes"
	"fmt"
	"io"
	"unicode"

	"github.com/jackc/pgx/pgio"
)

// Information on the internals of PostgreSQL arrays can be found in
// src/include/utils/array.h and src/backend/utils/adt/arrayfuncs.c. Of
// particular interest is the array_send function.

type ArrayHeader struct {
	ContainsNull bool
	ElementOID   int32
	Dimensions   []ArrayDimension
}

type ArrayDimension struct {
	Length     int32
	LowerBound int32
}

func (ah *ArrayHeader) DecodeBinary(r io.Reader) error {
	numDims, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if numDims > 0 {
		ah.Dimensions = make([]ArrayDimension, numDims)
	}

	containsNull, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}
	ah.ContainsNull = containsNull == 1

	ah.ElementOID, err = pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	for i := range ah.Dimensions {
		ah.Dimensions[i].Length, err = pgio.ReadInt32(r)
		if err != nil {
			return err
		}

		ah.Dimensions[i].LowerBound, err = pgio.ReadInt32(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ah *ArrayHeader) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, int32(len(ah.Dimensions)))
	if err != nil {
		return err
	}

	var containsNull int32
	if ah.ContainsNull {
		containsNull = 1
	}
	_, err = pgio.WriteInt32(w, containsNull)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, ah.ElementOID)
	if err != nil {
		return err
	}

	for i := range ah.Dimensions {
		_, err = pgio.WriteInt32(w, ah.Dimensions[i].Length)
		if err != nil {
			return err
		}

		_, err = pgio.WriteInt32(w, ah.Dimensions[i].LowerBound)
		if err != nil {
			return err
		}
	}

	return nil
}

type UntypedTextArray struct {
	Elements   []string
	Dimensions []ArrayDimension
}

func ParseUntypedTextArray(src string) (*UntypedTextArray, error) {
	uta := &UntypedTextArray{}

	buf := bytes.NewBufferString(src)

	skipWhitespace(buf)

	r, _, err := buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("invalid array: %v", err)
	}

	var explicitBounds bool
	// Array has explicit bounds
	if r == '[' {

	}

	// Parse values
	if r != '{' {
		return nil, fmt.Errorf("invalid array, expected '{': %v", err)
	}
	if !explicitBounds {
		uta.Dimensions = append(uta.Dimensions, ArrayDimension{LowerBound: 1})
	}
	currentDimension := 0

	for currentDimension >= 0 {

	}

	switch r {
	case '(':
		utr.LowerType = Exclusive
	case '[':
		utr.LowerType = Inclusive
	default:
		return nil, fmt.Errorf("missing lower bound, instead got: %v", string(r))
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("invalid lower value: %v", err)
	}
	buf.UnreadRune()

	if r == ',' {
		utr.LowerType = Unbounded
	} else {
		utr.Lower, err = rangeParseValue(buf)
		if err != nil {
			return nil, fmt.Errorf("invalid lower value: %v", err)
		}
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("missing range separator: %v", err)
	}
	if r != ',' {
		return nil, fmt.Errorf("missing range separator: %v", r)
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("invalid upper value: %v", err)
	}
	buf.UnreadRune()

	if r == ')' || r == ']' {
		utr.UpperType = Unbounded
	} else {
		utr.Upper, err = rangeParseValue(buf)
		if err != nil {
			return nil, fmt.Errorf("invalid upper value: %v", err)
		}
	}

	r, _, err = buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("missing upper bound: %v", err)
	}
	switch r {
	case ')':
		utr.UpperType = Exclusive
	case ']':
		utr.UpperType = Inclusive
	default:
		return nil, fmt.Errorf("missing upper bound, instead got: %v", string(r))
	}

	skipWhitespace(buf)

	if buf.Len() > 0 {
		return nil, fmt.Errorf("unexpected trailing data: %v", buf.String())
	}

	return utr, nil
}

func skipWhitespace(buf *bytes.Buffer) {
	var r rune
	var err error
	for r, _, _ = buf.ReadRune(); unicode.IsSpace(r); r, _, _ = buf.ReadRune() {
	}

	if err != io.EOF {
		buf.UnreadRune()
	}
}

func rangeParseValue(buf *bytes.Buffer) (string, error) {
	r, _, err := buf.ReadRune()
	if err != nil {
		return "", err
	}
	if r == '"' {
		return rangeParseQuotedValue(buf)
	}
	buf.UnreadRune()

	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", err
		}

		switch r {
		case '\\':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", err
			}
		case ',', '[', ']', '(', ')':
			buf.UnreadRune()
			return s.String(), nil
		}

		s.WriteRune(r)
	}
}

func rangeParseQuotedValue(buf *bytes.Buffer) (string, error) {
	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", err
		}

		switch r {
		case '\\':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", err
			}
		case '"':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", err
			}
			if r != '"' {
				buf.UnreadRune()
				return s.String(), nil
			}
		}
		s.WriteRune(r)
	}
}
