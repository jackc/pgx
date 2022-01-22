package pgtype_test

import (
	"bytes"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func isExpectedEqHardwareAddr(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
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
	testutil.RunTranscodeTests(t, "macaddr", []testutil.TranscodeTestCase{
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
