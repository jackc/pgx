package pgtype_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgx/v4"
)

var recordTests = []struct {
	sql      string
	expected pgtype.Record
}{
	{
		sql: `select row()`,
		expected: pgtype.Record{
			Fields: []pgtype.Value{},
			Status: pgtype.Present,
		},
	},
	{
		sql: `select row('foo'::text, 42::int4)`,
		expected: pgtype.Record{
			Fields: []pgtype.Value{
				&pgtype.Text{String: "foo", Status: pgtype.Present},
				&pgtype.Int4{Int: 42, Status: pgtype.Present},
			},
			Status: pgtype.Present,
		},
	},
	{
		sql: `select row(100.0::float4, 1.09::float4)`,
		expected: pgtype.Record{
			Fields: []pgtype.Value{
				&pgtype.Float4{Float: 100, Status: pgtype.Present},
				&pgtype.Float4{Float: 1.09, Status: pgtype.Present},
			},
			Status: pgtype.Present,
		},
	},
	{
		sql: `select row('foo'::text, array[1, 2, null, 4]::int4[], 42::int4)`,
		expected: pgtype.Record{
			Fields: []pgtype.Value{
				&pgtype.Text{String: "foo", Status: pgtype.Present},
				&pgtype.Int4Array{
					Elements: []pgtype.Int4{
						{Int: 1, Status: pgtype.Present},
						{Int: 2, Status: pgtype.Present},
						{Status: pgtype.Null},
						{Int: 4, Status: pgtype.Present},
					},
					Dimensions: []pgtype.ArrayDimension{{Length: 4, LowerBound: 1}},
					Status:     pgtype.Present,
				},
				&pgtype.Int4{Int: 42, Status: pgtype.Present},
			},
			Status: pgtype.Present,
		},
	},
	{
		sql: `select row(null)`,
		expected: pgtype.Record{
			Fields: []pgtype.Value{
				&pgtype.Unknown{Status: pgtype.Null},
			},
			Status: pgtype.Present,
		},
	},
	{
		sql: `select null::record`,
		expected: pgtype.Record{
			Status: pgtype.Null,
		},
	},
}

func TestRecordTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	for i, tt := range recordTests {
		psName := fmt.Sprintf("test%d", i)
		_, err := conn.Prepare(context.Background(), psName, tt.sql)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(tt.sql, func(t *testing.T) {
			var result pgtype.Record
			if err := conn.QueryRow(context.Background(), psName, pgx.QueryResultFormats{pgx.BinaryFormatCode}).Scan(&result); err != nil {
				t.Errorf("%v", err)
				return
			}

			if !reflect.DeepEqual(tt.expected, result) {
				t.Errorf("expected %#v, got %#v", tt.expected, result)
			}
		})

	}
}

func TestRecordWithUnknownOID(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists floatrange;

create type floatrange as range (
  subtype = float8,
  subtype_diff = float8mi
);`)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(context.Background(), "drop type floatrange")

	var result pgtype.Record
	err = conn.QueryRow(context.Background(), "select row('foo'::text, floatrange(1, 10), 'bar'::text)").Scan(&result)
	if err == nil {
		t.Errorf("expected error but none")
	}
}

func TestRecordAssignTo(t *testing.T) {
	var valueSlice []pgtype.Value
	var interfaceSlice []interface{}

	simpleTests := []struct {
		src      pgtype.Record
		dst      interface{}
		expected interface{}
	}{
		{
			src: pgtype.Record{
				Fields: []pgtype.Value{
					&pgtype.Text{String: "foo", Status: pgtype.Present},
					&pgtype.Int4{Int: 42, Status: pgtype.Present},
				},
				Status: pgtype.Present,
			},
			dst: &valueSlice,
			expected: []pgtype.Value{
				&pgtype.Text{String: "foo", Status: pgtype.Present},
				&pgtype.Int4{Int: 42, Status: pgtype.Present},
			},
		},
		{
			src: pgtype.Record{
				Fields: []pgtype.Value{
					&pgtype.Text{String: "foo", Status: pgtype.Present},
					&pgtype.Int4{Int: 42, Status: pgtype.Present},
				},
				Status: pgtype.Present,
			},
			dst:      &interfaceSlice,
			expected: []interface{}{"foo", int32(42)},
		},
		{
			src:      pgtype.Record{Status: pgtype.Null},
			dst:      &valueSlice,
			expected: (([]pgtype.Value)(nil)),
		},
		{
			src:      pgtype.Record{Status: pgtype.Null},
			dst:      &interfaceSlice,
			expected: (([]interface{})(nil)),
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
