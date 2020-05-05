package pgtype

import errors "golang.org/x/xerrors"

// EnumType represents an enum type. In the normal pgtype model a Go type maps to a PostgreSQL type and an instance
// of a Go type maps to a PostgreSQL value of that type. EnumType is different in that an instance of EnumType
// represents a PostgreSQL type. The zero value is not usable -- NewEnumType must be used as a constructor. In general,
// an EnumType should not be used to represent a value. It should only be used as an encoder and decoder internal to
// ConnInfo.
type EnumType struct {
	String string
	Status Status

	pgTypeName string            // PostgreSQL type name
	members    []string          // enum members
	membersMap map[string]string // map to quickly lookup member and reuse string instead of allocating
}

// NewEnumType initializes a new EnumType. It retains a read-only reference to members. members must not be changed.
func NewEnumType(pgTypeName string, members []string) *EnumType {
	et := &EnumType{pgTypeName: pgTypeName, members: members}
	et.membersMap = make(map[string]string, len(members))
	for _, m := range members {
		et.membersMap[m] = m
	}
	return et
}

func (et *EnumType) CloneTypeValue() Value {
	return &EnumType{
		String: et.String,
		Status: et.Status,

		pgTypeName: et.pgTypeName,
		members:    et.members,
		membersMap: et.membersMap,
	}
}

func (et *EnumType) PgTypeName() string {
	return et.pgTypeName
}

func (et *EnumType) Members() []string {
	return et.members
}

// Set assigns src to dst. Set purposely does not check that src is a member. This allows continued error free
// operation in the event the PostgreSQL enum type is modified during a connection.
func (dst *EnumType) Set(src interface{}) error {
	if src == nil {
		dst.Status = Null
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
		dst.String = value
		dst.Status = Present
	case *string:
		if value == nil {
			dst.Status = Null
		} else {
			dst.String = *value
			dst.Status = Present
		}
	case []byte:
		if value == nil {
			dst.Status = Null
		} else {
			dst.String = string(value)
			dst.Status = Present
		}
	default:
		if originalSrc, ok := underlyingStringType(src); ok {
			return dst.Set(originalSrc)
		}
		return errors.Errorf("cannot convert %v to enum %s", value, dst.pgTypeName)
	}

	return nil
}

func (dst EnumType) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.String
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *EnumType) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *string:
			*v = src.String
			return nil
		case *[]byte:
			*v = make([]byte, len(src.String))
			copy(*v, src.String)
			return nil
		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
			return errors.Errorf("unable to assign to %T", dst)
		}
	case Null:
		return NullAssignTo(dst)
	}

	return errors.Errorf("cannot decode %#v into %T", src, dst)
}

func (dst *EnumType) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		dst.Status = Null
		return nil
	}

	// Lookup the string in membersMap to avoid an allocation.
	if s, found := dst.membersMap[string(src)]; found {
		dst.String = s
	} else {
		// If an enum type is modified after the initial connection it is possible to receive an unexpected value.
		// Gracefully handle this situation. Purposely NOT modifying members and membersMap to allow for sharing members
		// and membersMap between connections.
		dst.String = string(src)
	}
	dst.Status = Present

	return nil
}

func (dst *EnumType) DecodeBinary(ci *ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (src EnumType) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	case Undefined:
		return nil, errUndefined
	}

	return append(buf, src.String...), nil
}

func (src EnumType) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	return src.EncodeText(ci, buf)
}
