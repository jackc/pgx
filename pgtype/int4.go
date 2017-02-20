package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

type Int4 int32

func (i *Int4) ParseText(src string) error {
	n, err := strconv.ParseInt(src, 10, 32)
	if err != nil {
		return err
	}

	*i = Int4(n)
	return nil
}

func (i *Int4) ParseBinary(src []byte) error {
	if len(src) != 4 {
		return fmt.Errorf("invalid length for int4: %v", len(src))
	}

	*i = Int4(binary.BigEndian.Uint32(src))
	return nil
}

func (i Int4) EncodeText(w io.Writer) error {
	s := strconv.FormatInt(int64(i), 10)
	_, err := WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

func (i Int4) EncodeBinary(w io.Writer) error {
	_, err := WriteInt32(w, 4)
	if err != nil {
		return err
	}

	_, err = WriteInt32(w, int32(i))
	return err
}
