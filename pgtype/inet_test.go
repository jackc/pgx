package pgtype_test

import (
	"net"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/assert"
)

func TestInetTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "inet", []interface{}{
		&pgtype.Inet{IPNet: mustParseInet(t, "0.0.0.0/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "127.0.0.1/8"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "12.34.56.65/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "192.168.1.16/24"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "255.0.0.0/8"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "255.255.255.255/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "10.0.0.1"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "2607:f8b0:4009:80b::200e"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "::1/64"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "::/0"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "::1/128"), Valid: true},
		&pgtype.Inet{IPNet: mustParseInet(t, "2607:f8b0:4009:80b::200e/64"), Valid: true},
		&pgtype.Inet{},
	})
}

func TestCidrTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "cidr", []interface{}{
		&pgtype.Inet{IPNet: mustParseCIDR(t, "0.0.0.0/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "12.34.56.0/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "192.168.1.0/24"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "255.0.0.0/8"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "255.255.255.255/32"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "::/128"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "::/0"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "::1/128"), Valid: true},
		&pgtype.Inet{IPNet: mustParseCIDR(t, "2607:f8b0:4009:80b::200e/128"), Valid: true},
		&pgtype.Inet{},
	})
}

func TestInetSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Inet
	}{
		{source: mustParseCIDR(t, "127.0.0.1/32"), result: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}},
		{source: mustParseCIDR(t, "127.0.0.1/32").IP, result: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}},
		{source: "127.0.0.1/32", result: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}},
		{source: "1.2.3.4/24", result: pgtype.Inet{IPNet: &net.IPNet{IP: net.ParseIP("1.2.3.4"), Mask: net.CIDRMask(24, 32)}, Valid: true}},
		{source: "10.0.0.1", result: pgtype.Inet{IPNet: mustParseInet(t, "10.0.0.1"), Valid: true}},
		{source: "2607:f8b0:4009:80b::200e", result: pgtype.Inet{IPNet: mustParseInet(t, "2607:f8b0:4009:80b::200e"), Valid: true}},
		{source: net.ParseIP(""), result: pgtype.Inet{}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Inet
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		assert.Equalf(t, tt.result.Valid, r.Valid, "%d: Status", i)
		if tt.result.Valid {
			assert.Equalf(t, tt.result.IPNet.Mask, r.IPNet.Mask, "%d: IP", i)
			assert.Truef(t, tt.result.IPNet.IP.Equal(r.IPNet.IP), "%d: Mask", i)
		}
	}
}

func TestInetAssignTo(t *testing.T) {
	var ipnet net.IPNet
	var pipnet *net.IPNet
	var ip net.IP
	var pip *net.IP

	simpleTests := []struct {
		src      pgtype.Inet
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}, dst: &ipnet, expected: *mustParseCIDR(t, "127.0.0.1/32")},
		{src: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}, dst: &ip, expected: mustParseCIDR(t, "127.0.0.1/32").IP},
		{src: pgtype.Inet{}, dst: &pipnet, expected: ((*net.IPNet)(nil))},
		{src: pgtype.Inet{}, dst: &pip, expected: ((*net.IP)(nil))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); !reflect.DeepEqual(dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %#v, but result was %#v", i, tt.src, tt.expected, dst)
		}
	}

	pointerAllocTests := []struct {
		src      pgtype.Inet
		dst      interface{}
		expected interface{}
	}{
		{src: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}, dst: &pipnet, expected: *mustParseCIDR(t, "127.0.0.1/32")},
		{src: pgtype.Inet{IPNet: mustParseCIDR(t, "127.0.0.1/32"), Valid: true}, dst: &pip, expected: mustParseCIDR(t, "127.0.0.1/32").IP},
	}

	for i, tt := range pointerAllocTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Elem().Interface(); !reflect.DeepEqual(dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	errorTests := []struct {
		src pgtype.Inet
		dst interface{}
	}{
		{src: pgtype.Inet{IPNet: mustParseCIDR(t, "192.168.0.0/16"), Valid: true}, dst: &ip},
		{src: pgtype.Inet{}, dst: &ipnet},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
