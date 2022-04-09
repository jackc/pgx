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

		return bytes.Compare(aa, vv) == 0
	}
}

func TestMacaddrCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type macaddr")

	// Only testing known OID query exec modes as net.HardwareAddr could map to macaddr or macaddr8.
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "macaddr", []pgxtest.ValueRoundTripTest{
		{
			mustParseMacaddr(t, "01:23:45:67:89:ab"),
			new(net.HardwareAddr),
			isExpectedEqHardwareAddr(mustParseMacaddr(t, "01:23:45:67:89:ab")),
		},
		{
			"01:23:45:67:89:ab",
			new(net.HardwareAddr),
			isExpectedEqHardwareAddr(mustParseMacaddr(t, "01:23:45:67:89:ab")),
		},
		{
			mustParseMacaddr(t, "01:23:45:67:89:ab"),
			new(string),
			isExpectedEq("01:23:45:67:89:ab"),
		},
		{nil, new(*net.HardwareAddr), isExpectedEq((*net.HardwareAddr)(nil))},
	})
}
