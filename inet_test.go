package pgtype_test

import (
	"net"
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestInetTranscode(t *testing.T) {
	for _, pgTypeName := range []string{"inet", "cidr"} {
		testSuccessfulTranscode(t, pgTypeName, []interface{}{
			pgtype.Inet{IPNet: mustParseCidr(t, "0.0.0.0/32"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "12.34.56.0/32"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "192.168.1.0/24"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "255.0.0.0/8"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "255.255.255.255/32"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "::/128"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "::/0"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "::1/128"), Status: pgtype.Present},
			pgtype.Inet{IPNet: mustParseCidr(t, "2607:f8b0:4009:80b::200e/128"), Status: pgtype.Present},
			pgtype.Inet{Status: pgtype.Null},
		})
	}
}

func TestInetSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Inet
	}{
		{source: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Null}, result: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Null}},
		{source: mustParseCidr(t, "127.0.0.1/32"), result: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
		{source: mustParseCidr(t, "127.0.0.1/32").IP, result: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
		{source: "127.0.0.1/32", result: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var r pgtype.Inet
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
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
		{src: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}, dst: &ipnet, expected: *mustParseCidr(t, "127.0.0.1/32")},
		{src: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}, dst: &ip, expected: mustParseCidr(t, "127.0.0.1/32").IP},
		{src: pgtype.Inet{Status: pgtype.Null}, dst: &pipnet, expected: ((*net.IPNet)(nil))},
		{src: pgtype.Inet{Status: pgtype.Null}, dst: &pip, expected: ((*net.IP)(nil))},
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
		{src: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}, dst: &pipnet, expected: *mustParseCidr(t, "127.0.0.1/32")},
		{src: pgtype.Inet{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}, dst: &pip, expected: mustParseCidr(t, "127.0.0.1/32").IP},
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
		{src: pgtype.Inet{IPNet: mustParseCidr(t, "192.168.0.0/16"), Status: pgtype.Present}, dst: &ip},
		{src: pgtype.Inet{Status: pgtype.Null}, dst: &ipnet},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}
