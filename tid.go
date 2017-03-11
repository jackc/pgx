package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pgx/pgio"
)

// TID is PostgreSQL's Tuple Identifier type.
//
// When one does
//
// 	select ctid, * from some_table;
//
// it is the data type of the ctid hidden system column.
//
// It is currently implemented as a pair unsigned two byte integers.
// Its conversion functions can be found in src/backend/utils/adt/tid.c
// in the PostgreSQL sources.
type TID struct {
	BlockNumber  uint32
	OffsetNumber uint16
	Status       Status
}

func (dst *TID) DecodeText(src []byte) error {
	if src == nil {
		*dst = TID{Status: Null}
		return nil
	}

	if len(src) < 5 {
		return fmt.Errorf("invalid length for tid: %v", len(src))
	}

	parts := strings.SplitN(string(src[1:len(src)-1]), ",", 2)
	if len(parts) < 2 {
		return fmt.Errorf("invalid format for tid")
	}

	blockNumber, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return err
	}

	offsetNumber, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return err
	}

	*dst = TID{BlockNumber: uint32(blockNumber), OffsetNumber: uint16(offsetNumber), Status: Present}
	return nil
}

func (dst *TID) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = TID{Status: Null}
		return nil
	}

	if len(src) != 6 {
		return fmt.Errorf("invalid length for tid: %v", len(src))
	}

	*dst = TID{
		BlockNumber:  binary.BigEndian.Uint32(src),
		OffsetNumber: binary.BigEndian.Uint16(src[4:]),
		Status:       Present,
	}
	return nil
}

func (src TID) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, fmt.Sprintf(`(%d,%d)`, src.BlockNumber, src.OffsetNumber))
	return false, err
}

func (src TID) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteUint32(w, src.BlockNumber)
	if err != nil {
		return false, err
	}

	_, err = pgio.WriteUint16(w, src.OffsetNumber)
	return false, err
}
