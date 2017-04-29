package pgtype

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

type AclitemArray struct {
	Elements   []Aclitem
	Dimensions []ArrayDimension
	Status     Status
}

func (dst *AclitemArray) Set(src interface{}) error {
	switch value := src.(type) {

	case []string:
		if value == nil {
			*dst = AclitemArray{Status: Null}
		} else if len(value) == 0 {
			*dst = AclitemArray{Status: Present}
		} else {
			elements := make([]Aclitem, len(value))
			for i := range value {
				if err := elements[i].Set(value[i]); err != nil {
					return err
				}
			}
			*dst = AclitemArray{
				Elements:   elements,
				Dimensions: []ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
				Status:     Present,
			}
		}

	default:
		if originalSrc, ok := underlyingSliceType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Aclitem", value)
	}

	return nil
}

func (dst *AclitemArray) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *AclitemArray) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {

		case *[]string:
			*v = make([]string, len(src.Elements))
			for i := range src.Elements {
				if err := src.Elements[i].AssignTo(&((*v)[i])); err != nil {
					return err
				}
			}
			return nil

		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
		}
	case Null:
		return NullAssignTo(dst)
	}

	return fmt.Errorf("cannot decode %v into %T", src, dst)
}

func (dst *AclitemArray) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = AclitemArray{Status: Null}
		return nil
	}

	uta, err := ParseUntypedTextArray(string(src))
	if err != nil {
		return err
	}

	var elements []Aclitem

	if len(uta.Elements) > 0 {
		elements = make([]Aclitem, len(uta.Elements))

		for i, s := range uta.Elements {
			var elem Aclitem
			var elemSrc []byte
			if s != "NULL" {
				elemSrc = []byte(s)
			}
			err = elem.DecodeText(ci, elemSrc)
			if err != nil {
				return err
			}

			elements[i] = elem
		}
	}

	*dst = AclitemArray{Elements: elements, Dimensions: uta.Dimensions, Status: Present}

	return nil
}

func (src *AclitemArray) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
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
		null, err := elem.EncodeText(ci, elemBuf)
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

// Scan implements the database/sql Scanner interface.
func (dst *AclitemArray) Scan(src interface{}) error {
	if src == nil {
		return dst.DecodeText(nil, nil)
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src *AclitemArray) Value() (driver.Value, error) {
	buf := &bytes.Buffer{}
	null, err := src.EncodeText(nil, buf)
	if err != nil {
		return nil, err
	}
	if null {
		return nil, nil
	}

	return buf.String(), nil
}
