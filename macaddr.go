package pgtype

import (
	"database/sql/driver"
	"fmt"
	"net"
)

type Macaddr struct {
	Addr  net.HardwareAddr
	Valid bool
}

func (dst *Macaddr) Set(src interface{}) error {
	if src == nil {
		*dst = Macaddr{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case net.HardwareAddr:
		addr := make(net.HardwareAddr, len(value))
		copy(addr, value)
		*dst = Macaddr{Addr: addr, Valid: true}
	case string:
		addr, err := net.ParseMAC(value)
		if err != nil {
			return err
		}
		*dst = Macaddr{Addr: addr, Valid: true}
	case *net.HardwareAddr:
		if value == nil {
			*dst = Macaddr{}
		} else {
			return dst.Set(*value)
		}
	case *string:
		if value == nil {
			*dst = Macaddr{}
		} else {
			return dst.Set(*value)
		}
	default:
		if originalSrc, ok := underlyingPtrType(src); ok {
			return dst.Set(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Macaddr", value)
	}

	return nil
}

func (dst Macaddr) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.Addr
}

func (src *Macaddr) AssignTo(dst interface{}) error {
	if !src.Valid {
		return NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *net.HardwareAddr:
		*v = make(net.HardwareAddr, len(src.Addr))
		copy(*v, src.Addr)
		return nil
	case *string:
		*v = src.Addr.String()
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}
}

func (dst *Macaddr) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Macaddr{}
		return nil
	}

	addr, err := net.ParseMAC(string(src))
	if err != nil {
		return err
	}

	*dst = Macaddr{Addr: addr, Valid: true}
	return nil
}

func (dst *Macaddr) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Macaddr{}
		return nil
	}

	if len(src) != 6 {
		return fmt.Errorf("Received an invalid size for a macaddr: %d", len(src))
	}

	addr := make(net.HardwareAddr, 6)
	copy(addr, src)

	*dst = Macaddr{Addr: addr, Valid: true}

	return nil
}

func (src Macaddr) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, src.Addr.String()...), nil
}

// EncodeBinary encodes src into w.
func (src Macaddr) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	return append(buf, src.Addr...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Macaddr) Scan(src interface{}) error {
	if src == nil {
		*dst = Macaddr{}
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
func (src Macaddr) Value() (driver.Value, error) {
	return EncodeValueText(src)
}
