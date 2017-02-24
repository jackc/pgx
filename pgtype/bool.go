package pgtype

import (
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

type Bool bool

func (b *Bool) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 1 {
		return fmt.Errorf("invalid length for bool: %v", size)
	}

	byt, err := pgio.ReadByte(r)
	if err != nil {
		return err
	}

	*b = Bool(byt == 't')
	return nil
}

func (b *Bool) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 1 {
		return fmt.Errorf("invalid length for bool: %v", size)
	}

	byt, err := pgio.ReadByte(r)
	if err != nil {
		return err
	}

	*b = Bool(byt == 1)
	return nil
}

func (b Bool) EncodeText(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 1)
	if err != nil {
		return nil
	}

	var buf []byte
	if b {
		buf = []byte{'t'}
	} else {
		buf = []byte{'f'}
	}

	_, err = w.Write(buf)
	return err
}

func (b Bool) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 1)
	if err != nil {
		return nil
	}

	var buf []byte
	if b {
		buf = []byte{1}
	} else {
		buf = []byte{0}
	}

	_, err = w.Write(buf)
	return err
}
