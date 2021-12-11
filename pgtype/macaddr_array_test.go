package pgtype_test

import (
	"net"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestMacaddrArrayTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "macaddr[]", []interface{}{
		&pgtype.MacaddrArray{
			Elements:   nil,
			Dimensions: nil,
			Valid:      true,
		},
		&pgtype.MacaddrArray{
			Elements: []pgtype.Macaddr{
				{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
				{},
			},
			Dimensions: []pgtype.ArrayDimension{{Length: 2, LowerBound: 1}},
			Valid:      true,
		},
		&pgtype.MacaddrArray{},
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
				Elements:   []pgtype.Macaddr{{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true},
		},
		{
			source: (([]net.HardwareAddr)(nil)),
			result: pgtype.MacaddrArray{},
		},
		{
			source: [][]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
			result: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
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
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Valid: true},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Valid: true},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Valid: true},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
		},
		{
			source: [2][1]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
			result: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
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
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Valid: true},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Valid: true},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Valid: true},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
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
				Elements:   []pgtype.Macaddr{{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &macaddrSlice,
			expected: []net.HardwareAddr{mustParseMacaddr(t, "01:23:45:67:89:ab")},
		},
		{
			src: pgtype.MacaddrArray{
				Elements:   []pgtype.Macaddr{{}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &macaddrSlice,
			expected: []net.HardwareAddr{nil},
		},
		{
			src:      pgtype.MacaddrArray{},
			dst:      &macaddrSlice,
			expected: (([]net.HardwareAddr)(nil)),
		},
		{
			src:      pgtype.MacaddrArray{Valid: true},
			dst:      &macaddrSlice,
			expected: []net.HardwareAddr{},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &macaddrSliceDim2,
			expected: [][]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Valid: true},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Valid: true},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Valid: true},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
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
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true},
			dst: &macaddrArrayDim2,
			expected: [2][1]net.HardwareAddr{
				{mustParseMacaddr(t, "01:23:45:67:89:ab")},
				{mustParseMacaddr(t, "cd:ef:01:23:45:67")}},
		},
		{
			src: pgtype.MacaddrArray{
				Elements: []pgtype.Macaddr{
					{Addr: mustParseMacaddr(t, "01:23:45:67:89:ab"), Valid: true},
					{Addr: mustParseMacaddr(t, "cd:ef:01:23:45:67"), Valid: true},
					{Addr: mustParseMacaddr(t, "89:ab:cd:ef:01:23"), Valid: true},
					{Addr: mustParseMacaddr(t, "45:67:89:ab:cd:ef"), Valid: true},
					{Addr: mustParseMacaddr(t, "fe:dc:ba:98:76:54"), Valid: true},
					{Addr: mustParseMacaddr(t, "32:10:fe:dc:ba:98"), Valid: true}},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true},
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
