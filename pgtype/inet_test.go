package pgtype_test

import (
	"context"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
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
		{mustParseInet(t, "0.0.0.0/32"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "0.0.0.0/32"))},
		{mustParseInet(t, "127.0.0.1/8"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "127.0.0.1/8"))},
		{mustParseInet(t, "12.34.56.65/32"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "12.34.56.65/32"))},
		{mustParseInet(t, "192.168.1.16/24"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "192.168.1.16/24"))},
		{mustParseInet(t, "255.0.0.0/8"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "255.0.0.0/8"))},
		{mustParseInet(t, "255.255.255.255/32"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "255.255.255.255/32"))},
		{mustParseInet(t, "2607:f8b0:4009:80b::200e"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "2607:f8b0:4009:80b::200e"))},
		{mustParseInet(t, "::1/64"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "::1/64"))},
		{mustParseInet(t, "::/0"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "::/0"))},
		{mustParseInet(t, "::1/128"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "::1/128"))},
		{mustParseInet(t, "2607:f8b0:4009:80b::200e/64"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "2607:f8b0:4009:80b::200e/64"))},
		{nil, new(pgtype.Inet), isExpectedEq(pgtype.Inet{})},
	})
}

func TestCidrTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support cidr type (see https://github.com/cockroachdb/cockroach/issues/18846)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "cidr", []pgxtest.ValueRoundTripTest{
		{mustParseInet(t, "0.0.0.0/32"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "0.0.0.0/32"))},
		{mustParseInet(t, "127.0.0.1/32"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "127.0.0.1/32"))},
		{mustParseInet(t, "12.34.56.0/32"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "12.34.56.0/32"))},
		{mustParseInet(t, "192.168.1.0/24"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "192.168.1.0/24"))},
		{mustParseInet(t, "255.0.0.0/8"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "255.0.0.0/8"))},
		{mustParseInet(t, "::/128"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "::/128"))},
		{mustParseInet(t, "::/0"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "::/0"))},
		{mustParseInet(t, "::1/128"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "::1/128"))},
		{mustParseInet(t, "2607:f8b0:4009:80b::200e/128"), new(net.IPNet), isExpectedEqIPNet(mustParseInet(t, "2607:f8b0:4009:80b::200e/128"))},
		{nil, new(pgtype.Inet), isExpectedEq(pgtype.Inet{})},
	})
}