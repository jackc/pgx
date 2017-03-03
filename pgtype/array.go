package pgtype

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
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

func (dst *ArrayHeader) DecodeBinary(r io.Reader) error {
	numDims, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if numDims > 0 {
		dst.Dimensions = make([]ArrayDimension, numDims)
	}

	containsNull, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}
	dst.ContainsNull = containsNull == 1

	dst.ElementOID, err = pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	for i := range dst.Dimensions {
		dst.Dimensions[i].Length, err = pgio.ReadInt32(r)
		if err != nil {
			return err
		}

		dst.Dimensions[i].LowerBound, err = pgio.ReadInt32(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (src *ArrayHeader) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, int32(len(src.Dimensions)))
	if err != nil {
		return err
	}

	var containsNull int32
	if src.ContainsNull {
		containsNull = 1
	}
	_, err = pgio.WriteInt32(w, containsNull)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, src.ElementOID)
	if err != nil {
		return err
	}

	for i := range src.Dimensions {
		_, err = pgio.WriteInt32(w, src.Dimensions[i].Length)
		if err != nil {
			return err
		}

		_, err = pgio.WriteInt32(w, src.Dimensions[i].LowerBound)
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
	dst := &UntypedTextArray{}

	buf := bytes.NewBufferString(src)

	skipWhitespace(buf)

	r, _, err := buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("invalid array: %v", err)
	}

	var explicitDimensions []ArrayDimension

	// Array has explicit dimensions
	if r == '[' {
		buf.UnreadRune()

		for {
			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			if r == '=' {
				break
			} else if r != '[' {
				return nil, fmt.Errorf("invalid array, expected '[' or '=' got %v", r)
			}

			lower, err := arrayParseInteger(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			if r != ':' {
				return nil, fmt.Errorf("invalid array, expected ':' got %v", r)
			}

			upper, err := arrayParseInteger(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %v", err)
			}

			if r != ']' {
				return nil, fmt.Errorf("invalid array, expected ']' got %v", r)
			}

			explicitDimensions = append(explicitDimensions, ArrayDimension{LowerBound: lower, Length: upper - lower + 1})
		}

		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}
	}

	if r != '{' {
		return nil, fmt.Errorf("invalid array, expected '{': %v", err)
	}

	implicitDimensions := []ArrayDimension{{LowerBound: 1, Length: 0}}

	// Consume all initial opening brackets. This provides number of dimensions.
	for {
		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}

		if r == '{' {
			implicitDimensions[len(implicitDimensions)-1].Length = 1
			implicitDimensions = append(implicitDimensions, ArrayDimension{LowerBound: 1})
		} else {
			buf.UnreadRune()
			break
		}
	}
	currentDim := len(implicitDimensions) - 1
	counterDim := currentDim

	for {
		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %v", err)
		}

		switch r {
		case '{':
			if currentDim == counterDim {
				implicitDimensions[currentDim].Length++
			}
			currentDim++
		case ',':
		case '}':
			currentDim--
			if currentDim < counterDim {
				counterDim = currentDim
			}
		default:
			buf.UnreadRune()
			value, err := arrayParseValue(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array value: %v", err)
			}
			if currentDim == counterDim {
				implicitDimensions[currentDim].Length++
			}
			dst.Elements = append(dst.Elements, value)
		}

		if currentDim < 0 {
			break
		}
	}

	skipWhitespace(buf)

	if buf.Len() > 0 {
		return nil, fmt.Errorf("unexpected trailing data: %v", buf.String())
	}

	if len(dst.Elements) == 0 {
		dst.Dimensions = nil
	} else if len(explicitDimensions) > 0 {
		dst.Dimensions = explicitDimensions
	} else {
		dst.Dimensions = implicitDimensions
	}

	return dst, nil
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

func arrayParseValue(buf *bytes.Buffer) (string, error) {
	r, _, err := buf.ReadRune()
	if err != nil {
		return "", err
	}
	if r == '"' {
		return arrayParseQuotedValue(buf)
	}
	buf.UnreadRune()

	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", err
		}

		switch r {
		case ',', '}':
			buf.UnreadRune()
			return s.String(), nil
		}

		s.WriteRune(r)
	}
}

func arrayParseQuotedValue(buf *bytes.Buffer) (string, error) {
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
			buf.UnreadRune()
			return s.String(), nil
		}
		s.WriteRune(r)
	}
}

func arrayParseInteger(buf *bytes.Buffer) (int32, error) {
	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return 0, err
		}

		if '0' <= r && r <= '9' {
			s.WriteRune(r)
		} else {
			buf.UnreadRune()
			n, err := strconv.ParseInt(s.String(), 10, 32)
			if err != nil {
				return 0, err
			}
			return int32(n), nil
		}
	}
}

func EncodeTextArrayDimensions(w io.Writer, dimensions []ArrayDimension) error {
	var customDimensions bool
	for _, dim := range dimensions {
		if dim.LowerBound != 1 {
			customDimensions = true
		}
	}

	if !customDimensions {
		return nil
	}

	for _, dim := range dimensions {
		err := pgio.WriteByte(w, '[')
		if err != nil {
			return err
		}

		_, err = io.WriteString(w, strconv.FormatInt(int64(dim.LowerBound), 10))
		if err != nil {
			return err
		}

		err = pgio.WriteByte(w, ':')
		if err != nil {
			return err
		}

		_, err = io.WriteString(w, strconv.FormatInt(int64(dim.LowerBound+dim.Length-1), 10))
		if err != nil {
			return err
		}

		err = pgio.WriteByte(w, ']')
		if err != nil {
			return err
		}
	}

	return pgio.WriteByte(w, '=')
}
