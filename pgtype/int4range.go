package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

type Int4range struct {
	Lower     int32
	Upper     int32
	LowerType BoundType
	UpperType BoundType
}

func (r *Int4range) ParseText(src string) error {
	utr, err := ParseUntypedTextRange(src)
	if err != nil {
		return err
	}

	r.LowerType = utr.LowerType
	r.UpperType = utr.UpperType

	if r.LowerType == Empty {
		return nil
	}

	if r.LowerType == Inclusive || r.LowerType == Exclusive {
		n, err := strconv.ParseInt(utr.Lower, 10, 32)
		if err != nil {
			return err
		}
		r.Lower = int32(n)
	}

	if r.UpperType == Inclusive || r.UpperType == Exclusive {
		n, err := strconv.ParseInt(utr.Upper, 10, 32)
		if err != nil {
			return err
		}
		r.Upper = int32(n)
	}

	return nil
}

func (r *Int4range) ParseBinary(src []byte) error {
	ubr, err := ParseUntypedBinaryRange(src)
	if err != nil {
		return err
	}

	r.LowerType = ubr.LowerType
	r.UpperType = ubr.UpperType

	if r.LowerType == Empty {
		return nil
	}

	if r.LowerType == Inclusive || r.LowerType == Exclusive {
		if len(ubr.Lower) != 4 {
			return fmt.Errorf("invalid length for lower int4: %v", len(ubr.Lower))
		}
		r.Lower = int32(binary.BigEndian.Uint32(ubr.Lower))
	}

	if r.UpperType == Inclusive || r.UpperType == Exclusive {
		if len(ubr.Upper) != 4 {
			return fmt.Errorf("invalid length for upper int4: %v", len(ubr.Upper))
		}
		r.Upper = int32(binary.BigEndian.Uint32(ubr.Upper))
	}

	return nil
}

func (r *Int4range) FormatText(w io.Writer) error {
	return nil
}

func (r *Int4range) FormatBinary(w io.Writer) error {
	return nil
}
