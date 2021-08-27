package pgtype

import (
	"database/sql/driver"
	"fmt"
)

// ACLItem is used for PostgreSQL's aclitem data type. A sample aclitem
// might look like this:
//
//	postgres=arwdDxt/postgres
//
// Note, however, that because the user/role name part of an aclitem is
// an identifier, it follows all the usual formatting rules for SQL
// identifiers: if it contains spaces and other special characters,
// it should appear in double-quotes:
//
//	postgres=arwdDxt/"role with spaces"
//
type ACLItem struct {
	String string
	Valid  bool
}

func (dst *ACLItem) Set(src interface{}) error {
	if src == nil {
		*dst = ACLItem{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case string:
		*dst = ACLItem{String: value, Valid: true}
	case *string:
		if value == nil {
			*dst = ACLItem{}
		} else {
			*dst = ACLItem{String: *value, Valid: true}
		}
	default:
		if originalSrc, ok := underlyingStringType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to ACLItem", value)
	}

	return nil
}

func (dst ACLItem) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.String
}

func (src *ACLItem) AssignTo(dst interface{}) error {
	if !src.Valid {
		return NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *string:
		*v = src.String
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}

	return fmt.Errorf("cannot decode %#v into %T", src, dst)
}

func (dst *ACLItem) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = ACLItem{}
		return nil
	}

	*dst = ACLItem{String: string(src), Valid: true}
	return nil
}

func (src ACLItem) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, src.String...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *ACLItem) Scan(src interface{}) error {
	if src == nil {
		*dst = ACLItem{}
		return nil
	}

	switch src := src.(type) {
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
func (src ACLItem) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	return src.String, nil
}
