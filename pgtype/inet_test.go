package pgtype_test

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqIPNet(a any) func(any) bool {
	return func(v any) bool {
		ap := a.(*net.IPNet)
		vp := v.(net.IPNet)

		return ap.IP.Equal(vp.IP) && ap.Mask.String() == vp.Mask.String()
	}
}

func TestInetTranscode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "inet", []pgxtest.ValueRoundTripTest{
		{Param: mustParseInet(t, "0.0.0.0/32"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "0.0.0.0/32"))},
		{Param: mustParseInet(t, "127.0.0.1/8"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "127.0.0.1/8"))},
		{Param: mustParseInet(t, "12.34.56.65/32"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "12.34.56.65/32"))},
		{Param: mustParseInet(t, "192.168.1.16/24"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "192.168.1.16/24"))},
		{Param: mustParseInet(t, "255.0.0.0/8"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "255.0.0.0/8"))},
		{Param: mustParseInet(t, "255.255.255.255/32"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "255.255.255.255/32"))},
		{Param: mustParseInet(t, "2607:f8b0:4009:80b::200e"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "2607:f8b0:4009:80b::200e"))},
		{Param: mustParseInet(t, "::1/64"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "::1/64"))},
		{Param: mustParseInet(t, "::/0"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "::/0"))},
		{Param: mustParseInet(t, "::1/128"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "::1/128"))},
		{Param: mustParseInet(t, "2607:f8b0:4009:80b::200e/64"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "2607:f8b0:4009:80b::200e/64"))},

		{Param: mustParseInet(t, "0.0.0.0/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("0.0.0.0/32"))},
		{Param: mustParseInet(t, "127.0.0.1/8"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("127.0.0.1/8"))},
		{Param: mustParseInet(t, "12.34.56.65/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("12.34.56.65/32"))},
		{Param: mustParseInet(t, "192.168.1.16/24"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("192.168.1.16/24"))},
		{Param: mustParseInet(t, "255.0.0.0/8"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("255.0.0.0/8"))},
		{Param: mustParseInet(t, "255.255.255.255/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("255.255.255.255/32"))},
		{Param: mustParseInet(t, "2607:f8b0:4009:80b::200e"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("2607:f8b0:4009:80b::200e/128"))},
		{Param: mustParseInet(t, "::1/64"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::1/64"))},
		{Param: mustParseInet(t, "::/0"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::/0"))},
		{Param: mustParseInet(t, "::1/128"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::1/128"))},
		{Param: mustParseInet(t, "2607:f8b0:4009:80b::200e/64"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("2607:f8b0:4009:80b::200e/64"))},

		{Param: netip.MustParsePrefix("0.0.0.0/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("0.0.0.0/32"))},
		{Param: netip.MustParsePrefix("127.0.0.1/8"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("127.0.0.1/8"))},
		{Param: netip.MustParsePrefix("12.34.56.65/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("12.34.56.65/32"))},
		{Param: netip.MustParsePrefix("192.168.1.16/24"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("192.168.1.16/24"))},
		{Param: netip.MustParsePrefix("255.0.0.0/8"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("255.0.0.0/8"))},
		{Param: netip.MustParsePrefix("255.255.255.255/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("255.255.255.255/32"))},
		{Param: netip.MustParsePrefix("::1/64"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::1/64"))},
		{Param: netip.MustParsePrefix("::/0"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::/0"))},
		{Param: netip.MustParsePrefix("::1/128"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::1/128"))},
		{Param: netip.MustParsePrefix("2607:f8b0:4009:80b::200e/64"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("2607:f8b0:4009:80b::200e/64"))},

		{Param: netip.MustParseAddr("0.0.0.0"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("0.0.0.0"))},
		{Param: netip.MustParseAddr("127.0.0.1"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("127.0.0.1"))},
		{Param: netip.MustParseAddr("12.34.56.65"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("12.34.56.65"))},
		{Param: netip.MustParseAddr("192.168.1.16"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("192.168.1.16"))},
		{Param: netip.MustParseAddr("255.0.0.0"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("255.0.0.0"))},
		{Param: netip.MustParseAddr("255.255.255.255"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("255.255.255.255"))},
		{Param: netip.MustParseAddr("2607:f8b0:4009:80b::200e"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("2607:f8b0:4009:80b::200e"))},
		{Param: netip.MustParseAddr("::1"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("::1"))},
		{Param: netip.MustParseAddr("::"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("::"))},
		{Param: netip.MustParseAddr("2607:f8b0:4009:80b::200e"), Result: new(netip.Addr), Test: isExpectedEq(netip.MustParseAddr("2607:f8b0:4009:80b::200e"))},

		{Param: nil, Result: new(netip.Prefix), Test: isExpectedEq(netip.Prefix{})},
	})
}

func TestCidrTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support cidr type (see https://github.com/cockroachdb/cockroach/issues/18846)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "cidr", []pgxtest.ValueRoundTripTest{
		{Param: mustParseInet(t, "0.0.0.0/32"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "0.0.0.0/32"))},
		{Param: mustParseInet(t, "127.0.0.1/32"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "127.0.0.1/32"))},
		{Param: mustParseInet(t, "12.34.56.0/32"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "12.34.56.0/32"))},
		{Param: mustParseInet(t, "192.168.1.0/24"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "192.168.1.0/24"))},
		{Param: mustParseInet(t, "255.0.0.0/8"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "255.0.0.0/8"))},
		{Param: mustParseInet(t, "::/128"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "::/128"))},
		{Param: mustParseInet(t, "::/0"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "::/0"))},
		{Param: mustParseInet(t, "::1/128"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "::1/128"))},
		{Param: mustParseInet(t, "2607:f8b0:4009:80b::200e/128"), Result: new(net.IPNet), Test: isExpectedEqIPNet(mustParseInet(t, "2607:f8b0:4009:80b::200e/128"))},

		{Param: netip.MustParsePrefix("0.0.0.0/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("0.0.0.0/32"))},
		{Param: netip.MustParsePrefix("127.0.0.1/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("127.0.0.1/32"))},
		{Param: netip.MustParsePrefix("12.34.56.0/32"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("12.34.56.0/32"))},
		{Param: netip.MustParsePrefix("192.168.1.0/24"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("192.168.1.0/24"))},
		{Param: netip.MustParsePrefix("255.0.0.0/8"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("255.0.0.0/8"))},
		{Param: netip.MustParsePrefix("::/128"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::/128"))},
		{Param: netip.MustParsePrefix("::/0"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::/0"))},
		{Param: netip.MustParsePrefix("::1/128"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("::1/128"))},
		{Param: netip.MustParsePrefix("2607:f8b0:4009:80b::200e/128"), Result: new(netip.Prefix), Test: isExpectedEq(netip.MustParsePrefix("2607:f8b0:4009:80b::200e/128"))},

		{Param: nil, Result: new(netip.Prefix), Test: isExpectedEq(netip.Prefix{})},
	})
}
