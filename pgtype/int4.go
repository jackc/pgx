package pgtype

import (
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int4 int32

func (i *Int4) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int4: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(string(buf), 10, 32)
	if err != nil {
		return err
	}

	*i = Int4(n)
	return nil
}

func (i *Int4) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 4 {
		return fmt.Errorf("invalid length for int4: %v", size)
	}

	n, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	*i = Int4(n)
	return nil
}

func (i Int4) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int4) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, int32(i))
	return err
}
