package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
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

func (src Oid) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	return append(buf, strconv.FormatUint(uint64(src), 10)...), nil
}

func (src Oid) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	return pgio.AppendUint32(buf, uint32(src)), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Oid) Scan(src interface{}) error {
	if src == nil {
		return fmt.Errorf("cannot scan NULL into %T", src)
	}

	switch src := src.(type) {
	case int64:
		*dst = Oid(src)
		return nil
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Oid) Value() (driver.Value, error) {
	return int64(src), nil
}
