package pgtype_test

import (
	"net"
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestCidrArrayTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "cidr[]", []interface{}{
		&pgtype.CidrArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.CidrArray{
			Elements: []pgtype.Cidr{
				pgtype.Cidr{IPNet: mustParseCidr(t, "12.34.56.0/32"), Status: pgtype.Present},
				pgtype.Cidr{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.CidrArray{Status: pgtype.Null},
		&pgtype.CidrArray{
			Elements: []pgtype.Cidr{
				pgtype.Cidr{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present},
				pgtype.Cidr{IPNet: mustParseCidr(t, "12.34.56.0/32"), Status: pgtype.Present},
				pgtype.Cidr{IPNet: mustParseCidr(t, "192.168.0.1/32"), Status: pgtype.Present},
				pgtype.Cidr{IPNet: mustParseCidr(t, "2607:f8b0:4009:80b::200e/128"), Status: pgtype.Present},
				pgtype.Cidr{Status: pgtype.Null},
				pgtype.Cidr{IPNet: mustParseCidr(t, "255.0.0.0/8"), Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}, {Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.CidrArray{
			Elements: []pgtype.Cidr{
				pgtype.Cidr{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present},
				pgtype.Cidr{IPNet: mustParseCidr(t, "12.34.56.0/32"), Status: pgtype.Present},
				pgtype.Cidr{IPNet: mustParseCidr(t, "192.168.0.1/32"), Status: pgtype.Present},
				pgtype.Cidr{IPNet: mustParseCidr(t, "2607:f8b0:4009:80b::200e/128"), Status: pgtype.Present},
			},
			Dimensions: []pgtype.ArrayDimension{
				{Length: 2, LowerBound: 4},
				{Length: 2, LowerBound: 2},
			},
			Status: pgtype.Present,
		},
	})
}

func TestCidrArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.CidrArray
	}{
		{
			source: []*net.IPNet{mustParseCidr(t, "127.0.0.1/32")},
			result: pgtype.CidrArray{
				Elements:   []pgtype.Cidr{{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([]*net.IPNet)(nil)),
			result: pgtype.CidrArray{Status: pgtype.Null},
		},
		{
			source: []net.IP{mustParseCidr(t, "127.0.0.1/32").IP},
			result: pgtype.CidrArray{
				Elements:   []pgtype.Cidr{{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([]net.IP)(nil)),
			result: pgtype.CidrArray{Status: pgtype.Null},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.CidrArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestCidrArrayAssignTo(t *testing.T) {
	var ipnetSlice []*net.IPNet
	var ipSlice []net.IP

	simpleTests := []struct {
		src      pgtype.CidrArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.CidrArray{
				Elements:   []pgtype.Cidr{{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &ipnetSlice,
			expected: []*net.IPNet{mustParseCidr(t, "127.0.0.1/32")},
		},
		{
			src: pgtype.CidrArray{
				Elements:   []pgtype.Cidr{{Status: pgtype.Null}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &ipnetSlice,
			expected: []*net.IPNet{nil},
		},
		{
			src: pgtype.CidrArray{
				Elements:   []pgtype.Cidr{{IPNet: mustParseCidr(t, "127.0.0.1/32"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &ipSlice,
			expected: []net.IP{mustParseCidr(t, "127.0.0.1/32").IP},
		},
		{
			src: pgtype.CidrArray{
				Elements:   []pgtype.Cidr{{Status: pgtype.Null}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &ipSlice,
			expected: []net.IP{nil},
		},
		{
			src:      pgtype.CidrArray{Status: pgtype.Null},
			dst:      &ipnetSlice,
			expected: (([]*net.IPNet)(nil)),
		},
		{
			src:      pgtype.CidrArray{Status: pgtype.Null},
			dst:      &ipSlice,
			expected: (([]net.IP)(nil)),
		},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); !reflect.DeepEqual(dst, tt.expected) {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}
}
