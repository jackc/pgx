package pgtype_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgtype"
	"github.com/jackc/pgx/v4/pgtype/testutil"
)

func TestHstoreArrayTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	var hstoreOID uint32
	err := conn.QueryRow(context.Background(), "select t.oid from pg_type t where t.typname='hstore';").Scan(&hstoreOID)
	if err != nil {
		t.Fatalf("did not find hstore OID, %v", err)
	}
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: &pgtype.Hstore{}, Name: "hstore", OID: hstoreOID})

	var hstoreArrayOID uint32
	err = conn.QueryRow(context.Background(), "select t.oid from pg_type t where t.typname='_hstore';").Scan(&hstoreArrayOID)
	if err != nil {
		t.Fatalf("did not find _hstore OID, %v", err)
	}
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: &pgtype.HstoreArray{}, Name: "_hstore", OID: hstoreArrayOID})

	text := func(s string) pgtype.Text {
		return pgtype.Text{String: s, Valid: true}
	}

	values := []pgtype.Hstore{
		{Map: map[string]pgtype.Text{}, Valid: true},
		{Map: map[string]pgtype.Text{"foo": text("bar")}, Valid: true},
		{Map: map[string]pgtype.Text{"foo": text("bar"), "baz": text("quz")}, Valid: true},
		{Map: map[string]pgtype.Text{"NULL": text("bar")}, Valid: true},
		{Map: map[string]pgtype.Text{"foo": text("NULL")}, Valid: true},
		{},
	}

	specialStrings := []string{
		`"`,
		`'`,
		`\`,
		`\\`,
		`=>`,
		` `,
		`\ / / \\ => " ' " '`,
	}
	for _, s := range specialStrings {
		// Special key values
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{s + "foo": text("bar")}, Valid: true})         // at beginning
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{"foo" + s + "bar": text("bar")}, Valid: true}) // in middle
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{"foo" + s: text("bar")}, Valid: true})         // at end
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{s: text("bar")}, Valid: true})                 // is key

		// Special value values
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text(s + "bar")}, Valid: true})         // at beginning
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("foo" + s + "bar")}, Valid: true}) // in middle
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text("foo" + s)}, Valid: true})         // at end
		values = append(values, pgtype.Hstore{Map: map[string]pgtype.Text{"foo": text(s)}, Valid: true})                 // is key
	}

	src := &pgtype.HstoreArray{
		Elements:   values,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(values)), LowerBound: 1}},
		Valid:      true,
	}

	_, err = conn.Prepare(context.Background(), "test", "select $1::hstore[]")
	if err != nil {
		t.Fatal(err)
	}

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for _, fc := range formats {
		queryResultFormats := pgx.QueryResultFormats{fc.formatCode}
		vEncoder := testutil.ForceEncoder(src, fc.formatCode)
		if vEncoder == nil {
			t.Logf("%#v does not implement %v", src, fc.name)
			continue
		}

		var result pgtype.HstoreArray
		err := conn.QueryRow(context.Background(), "test", queryResultFormats, vEncoder).Scan(&result)
		if err != nil {
			t.Errorf("%v: %v", fc.name, err)
			continue
		}

		if result.Valid != src.Valid {
			t.Errorf("%v: expected Valid %v, got %v", fc.formatCode, src.Valid, result.Valid)
			continue
		}

		if len(result.Elements) != len(src.Elements) {
			t.Errorf("%v: expected %v elements, got %v", fc.formatCode, len(src.Elements), len(result.Elements))
			continue
		}

		for i := range result.Elements {
			a := src.Elements[i]
			b := result.Elements[i]

			if a.Valid != b.Valid {
				t.Errorf("%v element idx %d: expected Valid %v, got %v", fc.formatCode, i, a.Valid, b.Valid)
			}

			if len(a.Map) != len(b.Map) {
				t.Errorf("%v element idx %d: expected %v pairs, got %v", fc.formatCode, i, len(a.Map), len(b.Map))
			}

			for k := range a.Map {
				if a.Map[k] != b.Map[k] {
					t.Errorf("%v element idx %d: expected key %v to be %v, got %v", fc.formatCode, i, k, a.Map[k], b.Map[k])
				}
			}
		}
	}
}

func TestHstoreArraySet(t *testing.T) {
	successfulTests := []struct {
		src    interface{}
		result pgtype.HstoreArray
	}{
		{
			src: []map[string]string{{"foo": "bar"}},
			result: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
		},
		{
			src: [][]map[string]string{{{"foo": "bar"}}, {{"baz": "quz"}}},
			result: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true,
			},
		},
		{
			src: [][][][]map[string]string{
				{{{{"foo": "bar"}, {"baz": "quz"}, {"bar": "baz"}}}},
				{{{{"wibble": "wobble"}, {"wubble": "wabble"}, {"wabble": "wobble"}}}}},
			result: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"bar": {String: "baz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wibble": {String: "wobble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wubble": {String: "wabble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wabble": {String: "wobble", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true,
			},
		},
		{
			src: [2][1]map[string]string{{{"foo": "bar"}}, {{"baz": "quz"}}},
			result: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true,
			},
		},
		{
			src: [2][1][1][3]map[string]string{
				{{{{"foo": "bar"}, {"baz": "quz"}, {"bar": "baz"}}}},
				{{{{"wibble": "wobble"}, {"wubble": "wabble"}, {"wabble": "wobble"}}}}},
			result: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"bar": {String: "baz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wibble": {String: "wobble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wubble": {String: "wabble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wabble": {String: "wobble", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true,
			},
		},
	}

	for i, tt := range successfulTests {
		var dst pgtype.HstoreArray
		err := dst.Set(tt.src)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !reflect.DeepEqual(dst, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.src, tt.result, dst)
		}
	}
}

func TestHstoreArrayAssignTo(t *testing.T) {
	var hstoreSlice []map[string]string
	var hstoreSliceDim2 [][]map[string]string
	var hstoreSliceDim4 [][][][]map[string]string
	var hstoreArrayDim2 [2][1]map[string]string
	var hstoreArrayDim4 [2][1][1][3]map[string]string

	simpleTests := []struct {
		src      pgtype.HstoreArray
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &hstoreSlice,
			expected: []map[string]string{{"foo": "bar"}}},
		{
			src: pgtype.HstoreArray{}, dst: &hstoreSlice, expected: (([]map[string]string)(nil)),
		},
		{
			src: pgtype.HstoreArray{Valid: true}, dst: &hstoreSlice, expected: []map[string]string{},
		},
		{
			src: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &hstoreSliceDim2,
			expected: [][]map[string]string{{{"foo": "bar"}}, {{"baz": "quz"}}},
		},
		{
			src: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"bar": {String: "baz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wibble": {String: "wobble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wubble": {String: "wabble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wabble": {String: "wobble", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true,
			},
			dst: &hstoreSliceDim4,
			expected: [][][][]map[string]string{
				{{{{"foo": "bar"}, {"baz": "quz"}, {"bar": "baz"}}}},
				{{{{"wibble": "wobble"}, {"wubble": "wabble"}, {"wabble": "wobble"}}}}},
		},
		{
			src: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{{LowerBound: 1, Length: 2}, {LowerBound: 1, Length: 1}},
				Valid:      true,
			},
			dst:      &hstoreArrayDim2,
			expected: [2][1]map[string]string{{{"foo": "bar"}}, {{"baz": "quz"}}},
		},
		{
			src: pgtype.HstoreArray{
				Elements: []pgtype.Hstore{
					{
						Map:   map[string]pgtype.Text{"foo": {String: "bar", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"baz": {String: "quz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"bar": {String: "baz", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wibble": {String: "wobble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wubble": {String: "wabble", Valid: true}},
						Valid: true,
					},
					{
						Map:   map[string]pgtype.Text{"wabble": {String: "wobble", Valid: true}},
						Valid: true,
					},
				},
				Dimensions: []pgtype.ArrayDimension{
					{LowerBound: 1, Length: 2},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 1},
					{LowerBound: 1, Length: 3}},
				Valid: true,
			},
			dst: &hstoreArrayDim4,
			expected: [2][1][1][3]map[string]string{
				{{{{"foo": "bar"}, {"baz": "quz"}, {"bar": "baz"}}}},
				{{{{"wibble": "wobble"}, {"wubble": "wabble"}, {"wabble": "wobble"}}}}},
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
