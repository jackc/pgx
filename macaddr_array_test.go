package pgtype_test

import (
	"net"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestMacaddrArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "macaddr[]", []interface{}{
		&pgtype.MacaddrArray{
			Elements:   nil,
			Dimensions: nil,
			Status:     pgtype.Present,
		},
		&pgtype.MacaddrArray{
			Elements: []pgtype.Macaddr{
				{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
				{Status: pgtype.Null},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Status:     pgtype.Present,
		},
		&pgtype.MacaddrArray{Status: pgtype.Null},
	})
}

func TestMacaddrArraySet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.MacaddrArray
	}{
		{
			source: []net.HardwareAddr{mustParseMacaddr(t, "01:23:45:67:89:ab")},
			result: pgtype.MacaddrArray{
				Elements:   []pgtype.Macaddr{{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: (([]net.HardwareAddr)(nil)),
			result: pgtype.MacaddrArray{Status: pgtype.Null},
		},
		{
			source: [][]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
			result: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [][][][]net.HardwareAddr{
				{{{
					mustParseMacaddr(t, "01:23:45:67:89:ab"),
					mustParseMacaddr(t, "cd:ef:01:23:45:67"),
					mustParseMacaddr(t, "89:ab:cd:ef:01:23")}}},
				{{{
					mustParseMacaddr(t, "45:67:89:ab:cd:ef"),
					mustParseMacaddr(t, "fe:dc:ba:98:76:54"),
					mustParseMacaddr(t, "32:10:fe:dc:ba:98")}}}},
			result: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
		{
			source: [2][1]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
			result: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
		},
		{
			source: [2][1][1][3]net.HardwareAddr{
				{{{
					mustParseMacaddr(t, "01:23:45:67:89:ab"),
					mustParseMacaddr(t, "cd:ef:01:23:45:67"),
					mustParseMacaddr(t, "89:ab:cd:ef:01:23")}}},
				{{{
					mustParseMacaddr(t, "45:67:89:ab:cd:ef"),
					mustParseMacaddr(t, "fe:dc:ba:98:76:54"),
					mustParseMacaddr(t, "32:10:fe:dc:ba:98")}}}},
			result: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.MacaddrArray
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestMacaddrArrayAssignTo(t *testing.T) {
	var macaddrSlice []net.HardwareAddr
	var macaddrSliceDim2 [][]net.HardwareAddr
	var macaddrSliceDim4 [][][][]net.HardwareAddr
	var macaddrArrayDim2 [2][1]net.HardwareAddr
	var macaddrArrayDim4 [2][1][1][3]net.HardwareAddr

	simpleTests := []struct {
		src      pgtype.MacaddrArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.MacaddrArray{
				Elements:   []pgtype.Macaddr{{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &macaddrSlice,
			expected: []net.HardwareAddr{mustParseMacaddr(t, "01:23:45:67:89:ab")},
		},
		{
			src: pgtype.MacaddrArray{
				Elements:   []pgtype.Macaddr{{Status: pgtype.Null}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Status:     pgtype.Present,
			},
			dst:      &macaddrSlice,
			expected: []net.HardwareAddr{nil},
		},
		{
			src:      pgtype.MacaddrArray{Status: pgtype.Null},
			dst:      &macaddrSlice,
			expected: (([]net.HardwareAddr)(nil)),
		},
		{
			src:      pgtype.MacaddrArray{Status: pgtype.Present},
			dst:      &macaddrSlice,
			expected: []net.HardwareAddr{},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &macaddrSliceDim2,
			expected: [][]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			dst: &macaddrSliceDim4,
			expected: [][][][]net.HardwareAddr{
				{{{
					mustParseMacaddr(t, "01:23:45:67:89:ab"),
					mustParseMacaddr(t, "cd:ef:01:23:45:67"),
					mustParseMacaddr(t, "89:ab:cd:ef:01:23")}}},
				{{{
					mustParseMacaddr(t, "45:67:89:ab:cd:ef"),
					mustParseMacaddr(t, "fe:dc:ba:98:76:54"),
					mustParseMacaddr(t, "32:10:fe:dc:ba:98")}}}},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Status:     pgtype.Present},
			dst: &macaddrArrayDim2,
			expected: [2][1]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Status: pgtype.Present},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Status: pgtype.Present}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Status: pgtype.Present},
			dst: &macaddrArrayDim4,
			expected: [2][1][1][3]net.HardwareAddr{
				{{{
					mustParseMacaddr(t, "01:23:45:67:89:ab"),
					mustParseMacaddr(t, "cd:ef:01:23:45:67"),
					mustParseMacaddr(t, "89:ab:cd:ef:01:23")}}},
				{{{
					mustParseMacaddr(t, "45:67:89:ab:cd:ef"),
					mustParseMacaddr(t, "fe:dc:ba:98:76:54"),
					mustParseMacaddr(t, "32:10:fe:dc:ba:98")}}}},
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
