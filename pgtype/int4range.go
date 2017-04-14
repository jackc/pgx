package pgtype

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

type Int4range struct {
	Lower     Int4
	Upper     Int4
	LowerType BoundType
	UpperType BoundType
	Status    Status
}

func (dst *Int4range) Set(src interface{}) error {
	return fmt.Errorf("cannot convert %v to Int4range", src)
}

func (dst *Int4range) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Int4range) AssignTo(dst interface{}) error {
	return fmt.Errorf("cannot assign %v to %T", src, dst)
}

func (dst *Int4range) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Int4range{Status: Null}
		return nil
	}

	utr, err := ParseUntypedTextRange(string(src))
	if err != nil {
		return err
	}

	*dst = Int4range{Status: Present}

	dst.LowerType = utr.LowerType
	dst.UpperType = utr.UpperType

	if dst.LowerType == Empty {
		return nil
	}

	if dst.LowerType == Inclusive || dst.LowerType == Exclusive {
		if err := dst.Lower.DecodeText(ci, []byte(utr.Lower)); err != nil {
			return err
		}
	}

	if dst.UpperType == Inclusive || dst.UpperType == Exclusive {
		if err := dst.Upper.DecodeText(ci, []byte(utr.Upper)); err != nil {
			return err
		}
	}

	return nil
}

func (dst *Int4range) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Int4range{Status: Null}
		return nil
	}

	ubr, err := ParseUntypedBinaryRange(src)
	if err != nil {
		return err
	}

	*dst = Int4range{Status: Present}

	dst.LowerType = ubr.LowerType
	dst.UpperType = ubr.UpperType

	if dst.LowerType == Empty {
		return nil
	}

	if dst.LowerType == Inclusive || dst.LowerType == Exclusive {
		if err := dst.Lower.DecodeBinary(ci, ubr.Lower); err != nil {
			return err
		}
	}

	if dst.UpperType == Inclusive || dst.UpperType == Exclusive {
		if err := dst.Upper.DecodeBinary(ci, ubr.Upper); err != nil {
			return err
		}
	}

	return nil
}

func (src *Int4range) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	switch src.LowerType {
	case Exclusive, Unbounded:
		if err := pgio.WriteByte(w, '('); err != nil {
			return false, err
		}
	case Inclusive:
		if err := pgio.WriteByte(w, '['); err != nil {
			return false, err
		}
	case Empty:
		_, err := io.WriteString(w, "empty")
		return false, err
	default:
		return false, fmt.Errorf("unknown lower bound type %v", src.LowerType)
	}

	if src.LowerType != Unbounded {
		if null, err := src.Lower.EncodeText(ci, w); err != nil {
			return false, err
		} else if null {
			return false, fmt.Errorf("Lower cannot be null unless LowerType is Unbounded")
		}
	}

	if err := pgio.WriteByte(w, ','); err != nil {
		return false, err
	}

	if src.UpperType != Unbounded {
		if null, err := src.Upper.EncodeText(ci, w); err != nil {
			return false, err
		} else if null {
			return false, fmt.Errorf("Upper cannot be null unless UpperType is Unbounded")
		}
	}

	switch src.UpperType {
	case Exclusive, Unbounded:
		if err := pgio.WriteByte(w, ')'); err != nil {
			return false, err
		}
	case Inclusive:
		if err := pgio.WriteByte(w, ']'); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("unknown upper bound type %v", src.UpperType)
	}

	return false, nil
}

func (src *Int4range) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	var rangeType byte
	switch src.LowerType {
	case Inclusive:
		rangeType |= lowerInclusiveMask
	case Unbounded:
		rangeType |= lowerUnboundedMask
	case Exclusive:
	case Empty:
		err := pgio.WriteByte(w, emptyMask)
		return false, err
	default:
		return false, fmt.Errorf("unknown LowerType: %v", src.LowerType)
	}

	switch src.UpperType {
	case Inclusive:
		rangeType |= upperInclusiveMask
	case Unbounded:
		rangeType |= upperUnboundedMask
	case Exclusive:
	default:
		return false, fmt.Errorf("unknown UpperType: %v", src.UpperType)
	}

	if err := pgio.WriteByte(w, rangeType); err != nil {
		return false, err
	}

	valBuf := &bytes.Buffer{}

	if src.LowerType != Unbounded {
		null, err := src.Lower.EncodeBinary(ci, valBuf)
		if err != nil {
			return false, err
		}
		if null {
			return false, fmt.Errorf("Lower cannot be null unless LowerType is Unbounded")
		}

		_, err = pgio.WriteInt32(w, int32(valBuf.Len()))
		if err != nil {
			return false, err
		}
		_, err = valBuf.WriteTo(w)
		if err != nil {
			return false, err
		}
	}

	if src.UpperType != Unbounded {
		null, err := src.Upper.EncodeBinary(ci, valBuf)
		if err != nil {
			return false, err
		}
		if null {
			return false, fmt.Errorf("Upper cannot be null unless UpperType is Unbounded")
		}

		_, err = pgio.WriteInt32(w, int32(valBuf.Len()))
		if err != nil {
			return false, err
		}
		_, err = valBuf.WriteTo(w)
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Int4range) Scan(src interface{}) error {
	if src == nil {
		*dst = Int4range{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src *Int4range) Value() (driver.Value, error) {
	return encodeValueText(src)
}
