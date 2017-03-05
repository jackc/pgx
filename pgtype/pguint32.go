package pgtype

import (
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

// pguint32 is the core type that is used to implement PostgreSQL types such as
// CID and XID.
type pguint32 struct {
	Uint   uint32
	Status Status
}

// ConvertFrom converts from src to dst. Note that as pguint32 is not a general
// number type ConvertFrom does not do automatic type conversion as other number
// types do.
func (dst *pguint32) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case uint32:
		*dst = pguint32{Uint: value, Status: Present}
	default:
		return fmt.Errorf("cannot convert %v to pguint32", value)
	}

	return nil
}

// AssignTo assigns from src to dst. Note that as pguint32 is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *pguint32) AssignTo(dst interface{}) error {
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

func (dst *pguint32) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*dst = pguint32{Status: Null}
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

	*dst = pguint32{Uint: uint32(n), Status: Present}
	return nil
}

func (dst *pguint32) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*dst = pguint32{Status: Null}
		return nil
	}

	if size != 4 {
		return fmt.Errorf("invalid length: %v", size)
	}

	n, err := pgio.ReadUint32(r)
	if err != nil {
		return err
	}

	*dst = pguint32{Uint: n, Status: Present}
	return nil
}

func (src pguint32) EncodeText(w io.Writer) error {
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

func (src pguint32) EncodeBinary(w io.Writer) error {
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
