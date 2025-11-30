package pgtype_test

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqHardwareAddr(a any) func(any) bool {
	return func(v any) bool {
		aa := a.(net.HardwareAddr)
		vv := v.(net.HardwareAddr)

		if (aa == nil) != (vv == nil) {
			return false
		}

		if aa == nil {
			return true
		}

		return bytes.Equal(aa, vv)
	}
}

func TestMacaddrCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type macaddr")

	// Only testing known OID query exec modes as net.HardwareAddr could map to macaddr or macaddr8.
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "macaddr", []pgxtest.ValueRoundTripTest{
		{
			Param:  mustParseMacaddr(t, "01:23:45:67:89:ab"),
			Result: new(net.HardwareAddr),
			Test:   isExpectedEqHardwareAddr(mustParseMacaddr(t, "01:23:45:67:89:ab")),
		},
		{
			Param:  "01:23:45:67:89:ab",
			Result: new(net.HardwareAddr),
			Test:   isExpectedEqHardwareAddr(mustParseMacaddr(t, "01:23:45:67:89:ab")),
		},
		{
			Param:  mustParseMacaddr(t, "01:23:45:67:89:ab"),
			Result: new(string),
			Test:   isExpectedEq("01:23:45:67:89:ab"),
		},
		{Param: nil, Result: new(*net.HardwareAddr), Test: isExpectedEq((*net.HardwareAddr)(nil))},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "macaddr8", []pgxtest.ValueRoundTripTest{
		{
			Param:  mustParseMacaddr(t, "01:23:45:67:89:ab:01:08"),
			Result: new(net.HardwareAddr),
			Test:   isExpectedEqHardwareAddr(mustParseMacaddr(t, "01:23:45:67:89:ab:01:08")),
		},
		{
			Param:  "01:23:45:67:89:ab:01:08",
			Result: new(net.HardwareAddr),
			Test:   isExpectedEqHardwareAddr(mustParseMacaddr(t, "01:23:45:67:89:ab:01:08")),
		},
		{
			Param:  mustParseMacaddr(t, "01:23:45:67:89:ab:01:08"),
			Result: new(string),
			Test:   isExpectedEq("01:23:45:67:89:ab:01:08"),
		},
		{Param: nil, Result: new(*net.HardwareAddr), Test: isExpectedEq((*net.HardwareAddr)(nil))},
	})
}
