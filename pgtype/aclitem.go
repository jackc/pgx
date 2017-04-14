package pgtype

import (
	"database/sql/driver"
	"fmt"
	"io"
)

// Aclitem is used for PostgreSQL's aclitem data type. A sample aclitem
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
type Aclitem struct {
	String string
	Status Status
}

func (dst *Aclitem) Set(src interface{}) error {
	switch value := src.(type) {
	case string:
		*dst = Aclitem{String: value, Status: Present}
	case *string:
		if value == nil {
			*dst = Aclitem{Status: Null}
		} else {
			*dst = Aclitem{String: *value, Status: Present}
		}
	default:
		if originalSrc, ok := underlyingStringType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Aclitem", value)
	}

	return nil
}

func (dst *Aclitem) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.String
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Aclitem) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *string:
			*v = src.String
			return nil
		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
		}
	case Null:
		return NullAssignTo(dst)
	}

	return fmt.Errorf("cannot decode %v into %T", src, dst)
}

func (dst *Aclitem) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Aclitem{Status: Null}
		return nil
	}

	*dst = Aclitem{String: string(src), Status: Present}
	return nil
}

func (src *Aclitem) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := io.WriteString(w, src.String)
	return false, err
}

// Scan implements the database/sql Scanner interface.
func (dst *Aclitem) Scan(src interface{}) error {
	if src == nil {
		*dst = Aclitem{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src *Aclitem) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		return src.String, nil
	case Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}
