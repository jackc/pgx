package pgtype

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/jackc/pgx/pgio"
)

// Oid (Object Identifier Type) is, according to
// https://www.postgresql.org/docs/current/static/datatype-oid.html, used
// internally by PostgreSQL as a primary key for various system tables. It is
// currently implemented as an unsigned four-byte integer. Its definition can be
// found in src/include/postgres_ext.h in the PostgreSQL sources. Because it is
// so frequently required to be in a NOT NULL condition Oid cannot be NULL. To
// allow for NULL Oids use OidValue.
type Oid uint32

func (dst *Oid) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		return fmt.Errorf("cannot decode nil into Oid")
	}

	n, err := strconv.ParseUint(string(src), 10, 32)
	if err != nil {
		return err
	}

	*dst = Oid(n)
	return nil
}

func (dst *Oid) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		return fmt.Errorf("cannot decode nil into Oid")
	}

	if len(src) != 4 {
		return fmt.Errorf("invalid length: %v", len(src))
	}

	n := binary.BigEndian.Uint32(src)
	*dst = Oid(n)
	return nil
}

func (src Oid) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	_, err := io.WriteString(w, strconv.FormatUint(uint64(src), 10))
	return false, err
}

func (src Oid) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	_, err := pgio.WriteUint32(w, uint32(src))
	return false, err
}
