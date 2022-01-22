package pgtype

import (
	"database/sql/driver"
	"fmt"
	"net"
)

// Network address family is dependent on server socket.h value for AF_INET.
// In practice, all platforms appear to have the same value. See
// src/include/utils/inet.h for more information.
const (
	defaultAFInet  = 2
	defaultAFInet6 = 3
)

type InetScanner interface {
	ScanInet(v Inet) error
}

type InetValuer interface {
	InetValue() (Inet, error)
}

// Inet represents both inet and cidr PostgreSQL types.
type Inet struct {
	IPNet *net.IPNet
	Valid bool
}

func (inet *Inet) ScanInet(v Inet) error {
	*inet = v
	return nil
}

func (inet Inet) InetValue() (Inet, error) {
	return inet, nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Inet) Scan(src interface{}) error {
	if src == nil {
		*dst = Inet{}
		return nil
	}

	switch src := src.(type) {
	case string:
		return scanPlanTextAnyToInetScanner{}.Scan([]byte(src), dst)
	}

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Inet) Value() (driver.Value, error) {
	if !src.Valid {
		return nil, nil
	}

	buf, err := InetCodec{}.PlanEncode(nil, 0, TextFormatCode, src).Encode(src, nil)
	if err != nil {
		return nil, err
	}
	return string(buf), err
}

type InetCodec struct{}

func (InetCodec) FormatSupported(format int16) bool {
	return format == TextFormatCode || format == BinaryFormatCode
}

func (InetCodec) PreferredFormat() int16 {
	return BinaryFormatCode
}

func (InetCodec) PlanEncode(ci *ConnInfo, oid uint32, format int16, value interface{}) EncodePlan {
	if _, ok := value.(InetValuer); !ok {
		return nil
	}

	switch format {
	case BinaryFormatCode:
		return encodePlanInetCodecBinary{}
	case TextFormatCode:
		return encodePlanInetCodecText{}
	}

	return nil
}

type encodePlanInetCodecBinary struct{}

func (encodePlanInetCodecBinary) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	inet, err := value.(InetValuer).InetValue()
	if err != nil {
		return nil, err
	}

	if !inet.Valid {
		return nil, nil
	}

	var family byte
	switch len(inet.IPNet.IP) {
	case net.IPv4len:
		family = defaultAFInet
	case net.IPv6len:
		family = defaultAFInet6
	default:
		return nil, fmt.Errorf("Unexpected IP length: %v", len(inet.IPNet.IP))
	}

	buf = append(buf, family)

	ones, _ := inet.IPNet.Mask.Size()
	buf = append(buf, byte(ones))

	// is_cidr is ignored on server
	buf = append(buf, 0)

	buf = append(buf, byte(len(inet.IPNet.IP)))

	return append(buf, inet.IPNet.IP...), nil
}

type encodePlanInetCodecText struct{}

func (encodePlanInetCodecText) Encode(value interface{}, buf []byte) (newBuf []byte, err error) {
	inet, err := value.(InetValuer).InetValue()
	if err != nil {
		return nil, err
	}

	if !inet.Valid {
		return nil, nil
	}

	return append(buf, inet.IPNet.String()...), nil
}

func (InetCodec) PlanScan(ci *ConnInfo, oid uint32, format int16, target interface{}, actualTarget bool) ScanPlan {

	switch format {
	case BinaryFormatCode:
		switch target.(type) {
		case InetScanner:
			return scanPlanBinaryInetToInetScanner{}
		}
	case TextFormatCode:
		switch target.(type) {
		case InetScanner:
			return scanPlanTextAnyToInetScanner{}
		}
	}

	return nil
}

func (c InetCodec) DecodeDatabaseSQLValue(ci *ConnInfo, oid uint32, format int16, src []byte) (driver.Value, error) {
	return codecDecodeToTextFormat(c, ci, oid, format, src)
}

func (c InetCodec) DecodeValue(ci *ConnInfo, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var inet Inet
	err := codecScan(c, ci, oid, format, src, &inet)
	if err != nil {
		return nil, err
	}

	if !inet.Valid {
		return nil, nil
	}

	return inet.IPNet, nil
}

type scanPlanBinaryInetToInetScanner struct{}

func (scanPlanBinaryInetToInetScanner) Scan(src []byte, dst interface{}) error {
	scanner := (dst).(InetScanner)

	if src == nil {
		return scanner.ScanInet(Inet{})
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
	if ipv4 := ipnet.IP.To4(); ipv4 != nil {
		ipnet.IP = ipv4
	}
	ipnet.Mask = net.CIDRMask(int(bits), len(ipnet.IP)*8)

	return scanner.ScanInet(Inet{IPNet: &ipnet, Valid: true})
}

type scanPlanTextAnyToInetScanner struct{}

func (scanPlanTextAnyToInetScanner) Scan(src []byte, dst interface{}) error {
	scanner := (dst).(InetScanner)

	if src == nil {
		return scanner.ScanInet(Inet{})
	}

	var ipnet *net.IPNet
	var err error

	if ip := net.ParseIP(string(src)); ip != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			ip = ipv4
		}
		bitCount := len(ip) * 8
		mask := net.CIDRMask(bitCount, bitCount)
		ipnet = &net.IPNet{Mask: mask, IP: ip}
	} else {
		ip, ipnet, err = net.ParseCIDR(string(src))
		if err != nil {
			return err
		}
		if ipv4 := ip.To4(); ipv4 != nil {
			ip = ipv4
		}
		ones, _ := ipnet.Mask.Size()
		*ipnet = net.IPNet{IP: ip, Mask: net.CIDRMask(ones, len(ip)*8)}
	}

	return scanner.ScanInet(Inet{IPNet: ipnet, Valid: true})
}
