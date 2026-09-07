package pgtype

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5/internal/pgio"
)

// Information on the internals of PostgreSQL arrays can be found in
// src/include/utils/array.h and src/backend/utils/adt/arrayfuncs.c. Of
// particular interest is the array_send function.

type arrayHeader struct {
	ContainsNull bool
	ElementOID   uint32
	Dimensions   []ArrayDimension
}

type ArrayDimension struct {
	Length     int32
	LowerBound int32
}

// cardinality returns the number of elements in an array of dimensions size.
func cardinality(dimensions []ArrayDimension) int {
	if len(dimensions) == 0 {
		return 0
	}

	elementCount := int(dimensions[0].Length)
	for _, d := range dimensions[1:] {
		elementCount *= int(d.Length)
	}

	if elementCount < 0 {
		return 0
	}

	return elementCount
}

func (dst *arrayHeader) DecodeBinary(r *pgio.Reader) error {
	// Each dimension is 8 bytes, which also bounds the Dimensions allocation below.
	numDims := r.Count(8)
	if err := r.Err(); err != nil {
		return fmt.Errorf("array header: %w", err)
	}

	if numDims > 6 {
		return fmt.Errorf("array has too many dimensions: %d", numDims)
	}

	dst.ContainsNull = r.Uint32() == 1
	dst.ElementOID = r.Uint32()

	dst.Dimensions = make([]ArrayDimension, numDims)
	for i := range dst.Dimensions {
		dst.Dimensions[i].Length = r.Int32()
		dst.Dimensions[i].LowerBound = r.Int32()
	}

	if err := r.Err(); err != nil {
		return fmt.Errorf("array header: %w", err)
	}
	return nil
}

func (src arrayHeader) EncodeBinary(buf []byte) []byte {
	buf = pgio.AppendInt32(buf, int32(len(src.Dimensions)))

	var containsNull int32
	if src.ContainsNull {
		containsNull = 1
	}
	buf = pgio.AppendInt32(buf, containsNull)

	buf = pgio.AppendUint32(buf, src.ElementOID)

	for i := range src.Dimensions {
		buf = pgio.AppendInt32(buf, src.Dimensions[i].Length)
		buf = pgio.AppendInt32(buf, src.Dimensions[i].LowerBound)
	}

	return buf
}

type untypedTextArray struct {
	Elements   []string
	Quoted     []bool
	Dimensions []ArrayDimension
}

func parseUntypedTextArray(src string, delimiter byte) (*untypedTextArray, error) {
	if delimiter == 0 {
		delimiter = ','
	}

	dst := &untypedTextArray{
		Elements:   []string{},
		Quoted:     []bool{},
		Dimensions: []ArrayDimension{},
	}

	buf := bytes.NewBufferString(src)

	skipWhitespace(buf)

	r, _, err := buf.ReadRune()
	if err != nil {
		return nil, fmt.Errorf("invalid array: %w", err)
	}

	var explicitDimensions []ArrayDimension

	// Array has explicit dimensions
	if r == '[' {
		buf.UnreadRune()

		for {
			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %w", err)
			}

			if r == '=' {
				break
			} else if r != '[' {
				return nil, fmt.Errorf("invalid array, expected '[' or '=' got %v", r)
			}

			lower, err := arrayParseInteger(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array: %w", err)
			}

			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %w", err)
			}

			if r != ':' {
				return nil, fmt.Errorf("invalid array, expected ':' got %v", r)
			}

			upper, err := arrayParseInteger(buf)
			if err != nil {
				return nil, fmt.Errorf("invalid array: %w", err)
			}

			r, _, err = buf.ReadRune()
			if err != nil {
				return nil, fmt.Errorf("invalid array: %w", err)
			}

			if r != ']' {
				return nil, fmt.Errorf("invalid array, expected ']' got %v", r)
			}

			explicitDimensions = append(explicitDimensions, ArrayDimension{LowerBound: lower, Length: upper - lower + 1})
		}

		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %w", err)
		}
	}

	if r != '{' {
		return nil, fmt.Errorf("invalid array, expected '{' got %v", r)
	}

	implicitDimensions := []ArrayDimension{{LowerBound: 1, Length: 0}}

	// Consume all initial opening brackets. This provides number of dimensions.
	for {
		r, _, err = buf.ReadRune()
		if err != nil {
			return nil, fmt.Errorf("invalid array: %w", err)
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
			return nil, fmt.Errorf("invalid array: %w", err)
		}

		switch r {
		case '{':
			if currentDim == counterDim {
				implicitDimensions[currentDim].Length++
			}
			currentDim++
		case rune(delimiter):
		case '}':
			currentDim--
			if currentDim < counterDim {
				counterDim = currentDim
			}
		default:
			buf.UnreadRune()
			value, quoted, err := arrayParseValue(buf, delimiter)
			if err != nil {
				return nil, fmt.Errorf("invalid array value: %w", err)
			}
			if currentDim == counterDim {
				implicitDimensions[currentDim].Length++
			}
			dst.Quoted = append(dst.Quoted, quoted)
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

	switch {
	case len(dst.Elements) == 0:
	case len(explicitDimensions) > 0:
		dst.Dimensions = explicitDimensions
	default:
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

func arrayParseValue(buf *bytes.Buffer, delimiter byte) (string, bool, error) {
	r, _, err := buf.ReadRune()
	if err != nil {
		return "", false, err
	}
	if r == '"' {
		return arrayParseQuotedValue(buf)
	}
	buf.UnreadRune()

	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", false, err
		}

		switch r {
		case rune(delimiter), '}':
			buf.UnreadRune()
			return s.String(), false, nil
		}

		s.WriteRune(r)
	}
}

func arrayParseQuotedValue(buf *bytes.Buffer) (string, bool, error) {
	s := &bytes.Buffer{}

	for {
		r, _, err := buf.ReadRune()
		if err != nil {
			return "", false, err
		}

		switch r {
		case '\\':
			r, _, err = buf.ReadRune()
			if err != nil {
				return "", false, err
			}
		case '"':
			_, _, err = buf.ReadRune()
			if err != nil {
				return "", false, err
			}
			buf.UnreadRune()
			return s.String(), true, nil
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

		if ('0' <= r && r <= '9') || r == '-' {
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

func encodeTextArrayDimensions(buf []byte, dimensions []ArrayDimension) []byte {
	var customDimensions bool
	for _, dim := range dimensions {
		if dim.LowerBound != 1 {
			customDimensions = true
		}
	}

	if !customDimensions {
		return buf
	}

	for _, dim := range dimensions {
		buf = append(buf, '[')
		buf = append(buf, strconv.FormatInt(int64(dim.LowerBound), 10)...)
		buf = append(buf, ':')
		buf = append(buf, strconv.FormatInt(int64(dim.LowerBound+dim.Length-1), 10)...)
		buf = append(buf, ']')
	}

	return append(buf, '=')
}

var quoteArrayReplacer = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func quoteArrayElement(src string) string {
	return `"` + quoteArrayReplacer.Replace(src) + `"`
}

func quoteArrayElementIfNeeded(src string, delimiter byte) string {
	if delimiter == 0 {
		delimiter = ','
	}

	if src == "" || (len(src) == 4 && strings.EqualFold(src, "null")) || strings.ContainsAny(src, " \t\n\r\v\f") || strings.ContainsAny(src, `{},"\`) || strings.ContainsRune(src, rune(delimiter)) {
		return quoteArrayElement(src)
	}
	return src
}

// Array represents a PostgreSQL array for T. It implements the [ArrayGetter] and [ArraySetter] interfaces. It preserves
// PostgreSQL dimensions and custom lower bounds. Use [FlatArray] if these are not needed.
type Array[T any] struct {
	Elements []T
	Dims     []ArrayDimension
	Valid    bool
}

func (a Array[T]) Dimensions() []ArrayDimension {
	return a.Dims
}

func (a Array[T]) Index(i int) any {
	return a.Elements[i]
}

func (a Array[T]) IndexType() any {
	var el T
	return el
}

func (a *Array[T]) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		*a = Array[T]{}
		return nil
	}

	elementCount := cardinality(dimensions)
	*a = Array[T]{
		Elements: make([]T, elementCount),
		Dims:     dimensions,
		Valid:    true,
	}

	return nil
}

func (a Array[T]) ScanIndex(i int) any {
	return &a.Elements[i]
}

func (a Array[T]) ScanIndexType() any {
	return new(T)
}

// FlatArray implements the [ArrayGetter] and [ArraySetter] interfaces for any slice of T. It ignores PostgreSQL dimensions
// and custom lower bounds. Use [Array] to preserve these.
type FlatArray[T any] []T

func (a FlatArray[T]) Dimensions() []ArrayDimension {
	if a == nil {
		return nil
	}

	return []ArrayDimension{{Length: int32(len(a)), LowerBound: 1}}
}

func (a FlatArray[T]) Index(i int) any {
	return a[i]
}

func (a FlatArray[T]) IndexType() any {
	var el T
	return el
}

func (a *FlatArray[T]) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		*a = nil
		return nil
	}

	elementCount := cardinality(dimensions)
	*a = make(FlatArray[T], elementCount)
	return nil
}

func (a FlatArray[T]) ScanIndex(i int) any {
	return &a[i]
}

func (a FlatArray[T]) ScanIndexType() any {
	return new(T)
}
