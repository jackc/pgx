package pgtype

import (
	"fmt"
	"io"
	"net"
	"reflect"

	"github.com/jackc/pgx/pgio"
)

// Network address family is dependent on server socket.h value for AF_INET.
// In practice, all platforms appear to have the same value. See
// src/include/utils/inet.h for more information.
const (
	defaultAFInet  = 2
	defaultAFInet6 = 3
)

// Inet represents both inet and cidr PostgreSQL types.
type Inet struct {
	IPNet  *net.IPNet
	Status Status
}

func (dst *Inet) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Inet:
		*dst = value
	case net.IPNet:
		*dst = Inet{IPNet: &value, Status: Present}
	case *net.IPNet:
		*dst = Inet{IPNet: value, Status: Present}
	case net.IP:
		bitCount := len(value) * 8
		mask := net.CIDRMask(bitCount, bitCount)
		*dst = Inet{IPNet: &net.IPNet{Mask: mask, IP: value}, Status: Present}
	case string:
		_, ipnet, err := net.ParseCIDR(value)
		if err != nil {
			return err
		}
		*dst = Inet{IPNet: ipnet, Status: Present}
	default:
		if originalSrc, ok := underlyingPtrType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Inet", value)
	}

	return nil
}

func (src *Inet) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *net.IPNet:
		if src.Status != Present {
			return fmt.Errorf("cannot assign %v to %T", src, dst)
		}
		*v = *src.IPNet
	case *net.IP:
		if src.Status == Present {

			if oneCount, bitCount := src.IPNet.Mask.Size(); oneCount != bitCount {
				return fmt.Errorf("cannot assign %v to %T", src, dst)
			}
			*v = src.IPNet.IP
		} else {
			*v = nil
		}
	default:
		if v := reflect.ValueOf(dst); v.Kind() == reflect.Ptr {
			el := v.Elem()
			switch el.Kind() {
			// if dst is a pointer to pointer, strip the pointer and try again
			case reflect.Ptr:
				if src.Status == Null {
					el.Set(reflect.Zero(el.Type()))
					return nil
				}
				if el.IsNil() {
					// allocate destination
					el.Set(reflect.New(el.Type().Elem()))
				}
				return src.AssignTo(el.Interface())
			}
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *Inet) DecodeText(src []byte) error {
	if src == nil {
		*dst = Inet{Status: Null}
		return nil
	}

	var ipnet *net.IPNet
	var err error

	if ip := net.ParseIP(string(src)); ip != nil {
		ipv4 := ip.To4()
		if ipv4 != nil {
			ip = ipv4
		}
		bitCount := len(ip) * 8
		mask := net.CIDRMask(bitCount, bitCount)
		ipnet = &net.IPNet{Mask: mask, IP: ip}
	} else {
		_, ipnet, err = net.ParseCIDR(string(src))
		if err != nil {
			return err
		}
	}

	*dst = Inet{IPNet: ipnet, Status: Present}
	return nil
}

func (dst *Inet) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Inet{Status: Null}
		return nil
	}

	if len(src) != 8 && len(src) != 20 {
		return fmt.Errorf("Received an invalid size for a inet: %d", len(src))
	}

	// ignore family
	bits := src[1]
	// ignore is_cidr
	addressLength := src[3]

	var ipnet net.IPNet
	ipnet.IP = make(net.IP, int(addressLength))
	copy(ipnet.IP, src[4:])
	ipnet.Mask = net.CIDRMask(int(bits), int(addressLength)*8)

	*dst = Inet{IPNet: &ipnet, Status: Present}

	return nil
}

func (src Inet) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	s := src.IPNet.String()
	_, err := pgio.WriteInt32(w, int32(len(s)))
	if err != nil {
		return nil
	}
	_, err = w.Write([]byte(s))
	return err
}

// EncodeBinary encodes src into w.
func (src Inet) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	var size int32
	var family byte
	switch len(src.IPNet.IP) {
	case net.IPv4len:
		size = 8
		family = defaultAFInet
	case net.IPv6len:
		size = 20
		family = defaultAFInet6
	default:
		return fmt.Errorf("Unexpected IP length: %v", len(src.IPNet.IP))
	}

	if _, err := pgio.WriteInt32(w, size); err != nil {
		return err
	}

	if err := pgio.WriteByte(w, family); err != nil {
		return err
	}

	ones, _ := src.IPNet.Mask.Size()
	if err := pgio.WriteByte(w, byte(ones)); err != nil {
		return err
	}

	// is_cidr is ignored on server
	if err := pgio.WriteByte(w, 0); err != nil {
		return err
	}

	if err := pgio.WriteByte(w, byte(len(src.IPNet.IP))); err != nil {
		return err
	}

	_, err := w.Write(src.IPNet.IP)
	return err
}
