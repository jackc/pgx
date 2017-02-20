package pgtype

import (
	"encoding/binary"
	"fmt"
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

func (i Int4) FormatText() (string, error) {
	return strconv.FormatInt(int64(i), 10), nil
}

func (i Int4) FormatBinary() ([]byte, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf, nil
}
