package pgtype

import (
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

// CID is PostgreSQL's Command Identifier type.
//
// When one does
//
// 	select cmin, cmax, * from some_table;
//
// it is the data type of the cmin and cmax hidden system columns.
//
// It is currently implemented as an unsigned four byte integer.
// Its definition can be found in src/include/c.h as CommandId
// in the PostgreSQL sources.
type CID struct {
	Uint   uint32
	Status Status
}

// ConvertFrom converts from src to dst. Note that as CID is not a general
// number type ConvertFrom does not do automatic type conversion as other number
// types do.
func (dst *CID) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case CID:
		*dst = value
	case uint32:
		*dst = CID{Uint: value, Status: Present}
	default:
		return fmt.Errorf("cannot convert %v to CID", value)
	}

	return nil
}

// AssignTo assigns from src to dst. Note that as CID is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *CID) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *uint32:
		if src.Status == Present {
			*v = src.Uint
		} else {
			return fmt.Errorf("cannot assign %v into %T", src, dst)
		}
	case **uint32:
		if src.Status == Present {
			n := src.Uint
			*v = &n
		} else {
			*v = nil
		}
	}

	return nil
}

func (dst *CID) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*dst = CID{Status: Null}
		return nil
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseUint(string(buf), 10, 32)
	if err != nil {
		return err
	}

	*dst = CID{Uint: uint32(n), Status: Present}
	return nil
}

func (dst *CID) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*dst = CID{Status: Null}
		return nil
	}

	if size != 4 {
		return fmt.Errorf("invalid length for cid: %v", size)
	}

	n, err := pgio.ReadUint32(r)
	if err != nil {
		return err
	}

	*dst = CID{Uint: n, Status: Present}
	return nil
}

func (src CID) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	s := strconv.FormatUint(uint64(src.Uint), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (src CID) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	_, err = pgio.WriteUint32(w, src.Uint)
	return err
}
