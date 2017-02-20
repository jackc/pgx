package pgtype

import (
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

type Int2 int32

func (i *Int2) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int2: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	n, err := strconv.ParseInt(string(buf), 10, 16)
	if err != nil {
		return err
	}

	*i = Int2(n)
	return nil
}

func (i *Int2) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 2 {
		return fmt.Errorf("invalid length for int2: %v", size)
	}

	n, err := pgio.ReadInt16(r)
	if err != nil {
		return err
	}

	*i = Int2(n)
	return nil
}

func (i Int2) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int2) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 2)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt16(w, int16(i))
	return err
}
