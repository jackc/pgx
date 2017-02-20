package pgtype

import (
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int8 int64

func (i *Int8) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int8: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(string(buf), 10, 64)
	if err != nil {
		return err
	}

	*i = Int8(n)
	return nil
}

func (i *Int8) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 8 {
		return fmt.Errorf("invalid length for int8: %v", size)
	}

	n, err := pgio.ReadInt64(r)
	if err != nil {
		return err
	}

	*i = Int8(n)
	return nil
}

func (i Int8) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int8) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 8)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt64(w, int64(i))
	return err
}
