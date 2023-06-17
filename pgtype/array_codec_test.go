package pgtype_test

import (
	"context"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrayCodec(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for i, tt := range []struct {
			expected any
		}{
			{[]int16(nil)},
			{[]int16{}},
			{[]int16{1, 2, 3}},
		} {
			var actual []int16
			err := conn.QueryRow(
				ctx,
				"select $1::smallint[]",
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}

		newInt16 := func(n int16) *int16 { return &n }

		for i, tt := range []struct {
			expected any
		}{
			{[]*int16{newInt16(1), nil, newInt16(3), nil, newInt16(5)}},
		} {
			var actual []*int16
			err := conn.QueryRow(
				ctx,
				"select $1::smallint[]",
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	})
}

func TestArrayCodecFlatArrayString(t *testing.T) {
	testCases := []struct {
		input []string
	}{
		{nil},
		{[]string{}},
		{[]string{"a"}},
		{[]string{"a", "b"}},
		// previously had a bug with whitespace handling
		{[]string{"\v", "\t", "\n", "\r", "\f", " "}},
		{[]string{"a\vb", "a\tb", "a\nb", "a\rb", "a\fb", "a b"}},
	}

	queryModes := []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeDescribeExec}

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for i, testCase := range testCases {
			for _, queryMode := range queryModes {
				var out []string
				err := conn.QueryRow(ctx, "select $1::text[]", queryMode, testCase.input).Scan(&out)
				if err != nil {
					t.Fatalf("i=%d input=%#v queryMode=%s: Scan failed: %s",
						i, testCase.input, queryMode, err)
				}
				if !reflect.DeepEqual(out, testCase.input) {
					t.Errorf("i=%d input=%#v queryMode=%s: not equal output=%#v",
						i, testCase.input, queryMode, out)
				}
			}
		}
	})
}

func TestArrayCodecArray(t *testing.T) {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support multi-dimensional arrays")
	}

	ctr.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for i, tt := range []struct {
			expected any
		}{
			{pgtype.Array[int32]{
				Elements: []int32{1, 2, 3, 4},
				Dims: []pgtype.ArrayDimension{
					{Length: 2, LowerBound: 2},
					{Length: 2, LowerBound: 2},
				},
				Valid: true,
			}},
		} {
			var actual pgtype.Array[int32]
			err := conn.QueryRow(
				ctx,
				"select $1::int[]",
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	})
}

func TestArrayCodecNamedSliceType(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type _int16Slice []int16

		for i, tt := range []struct {
			expected any
		}{
			{_int16Slice(nil)},
			{_int16Slice{}},
			{_int16Slice{1, 2, 3}},
		} {
			var actual _int16Slice
			err := conn.QueryRow(
				ctx,
				"select $1::smallint[]",
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	})
}

// https://github.com/jackc/pgx/issues/1488
func TestArrayCodecAnySliceArgument(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type _int16Slice []int16

		for i, tt := range []struct {
			arg      any
			expected []int16
		}{
			{[]any{1, 2, 3}, []int16{1, 2, 3}},
		} {
			var actual []int16
			err := conn.QueryRow(
				ctx,
				"select $1::smallint[]",
				tt.arg,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	})
}

// https://github.com/jackc/pgx/issues/1442
func TestArrayCodecAnyArray(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		type _point3 [3]float32

		for i, tt := range []struct {
			expected any
		}{
			{_point3{0, 0, 0}},
			{_point3{1, 2, 3}},
		} {
			var actual _point3
			err := conn.QueryRow(
				ctx,
				"select $1::float4[]",
				tt.expected,
			).Scan(&actual)
			assert.NoErrorf(t, err, "%d", i)
			assert.Equalf(t, tt.expected, actual, "%d", i)
		}
	})
}

// https://github.com/jackc/pgx/issues/1273#issuecomment-1218262703
func TestArrayCodecSliceArgConversion(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		arg := []string{
			"3ad95bfd-ecea-4032-83c3-0c823cafb372",
			"951baf11-c0cc-4afc-a779-abff0611dbf1",
			"8327f244-7e2f-45e7-a10b-fbdc9d6f3378",
		}

		var expected []pgtype.UUID

		for _, s := range arg {
			buf, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
			require.NoError(t, err)
			var u pgtype.UUID
			copy(u.Bytes[:], buf)
			u.Valid = true
			expected = append(expected, u)
		}

		var actual []pgtype.UUID
		err := conn.QueryRow(
			ctx,
			"select $1::uuid[]",
			arg,
		).Scan(&actual)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	})
}

func TestArrayCodecDecodeValue(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		for _, tt := range []struct {
			sql      string
			expected any
		}{
			{
				sql:      `select '{}'::int4[]`,
				expected: []any{},
			},
			{
				sql:      `select '{1,2}'::int8[]`,
				expected: []any{int64(1), int64(2)},
			},
			{
				sql:      `select '{foo,bar}'::text[]`,
				expected: []any{"foo", "bar"},
			},
		} {
			t.Run(tt.sql, func(t *testing.T) {
				rows, err := conn.Query(ctx, tt.sql)
				require.NoError(t, err)

				for rows.Next() {
					values, err := rows.Values()
					require.NoError(t, err)
					require.Len(t, values, 1)
					require.Equal(t, tt.expected, values[0])
				}

				require.NoError(t, rows.Err())
			})
		}
	})
}

func TestArrayCodecScanMultipleDimensions(t *testing.T) {
	skipCockroachDB(t, "Server does not support nested arrays (https://github.com/cockroachdb/cockroach/issues/36815)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		rows, err := conn.Query(ctx, `select '{{1,2,3,4}, {5,6,7,8}, {9,10,11,12}}'::int4[]`)
		require.NoError(t, err)

		for rows.Next() {
			var ss [][]int32
			err := rows.Scan(&ss)
			require.NoError(t, err)
			require.Equal(t, [][]int32{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}, ss)
		}

		require.NoError(t, rows.Err())
	})
}

func TestArrayCodecScanMultipleDimensionsEmpty(t *testing.T) {
	skipCockroachDB(t, "Server does not support nested arrays (https://github.com/cockroachdb/cockroach/issues/36815)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select '{}'::int4[]`)
		require.NoError(t, err)

		for rows.Next() {
			var ss [][]int32
			err := rows.Scan(&ss)
			require.NoError(t, err)
			require.Equal(t, [][]int32{}, ss)
		}

		require.NoError(t, rows.Err())
	})
}

func TestArrayCodecScanWrongMultipleDimensions(t *testing.T) {
	skipCockroachDB(t, "Server does not support nested arrays (https://github.com/cockroachdb/cockroach/issues/36815)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select '{{1,2,3,4}, {5,6,7,8}, {9,10,11,12}}'::int4[]`)
		require.NoError(t, err)

		for rows.Next() {
			var ss [][][]int32
			err := rows.Scan(&ss)
			require.Error(t, err, "can't scan into dest[0]: PostgreSQL array has 2 dimensions but slice has 3 dimensions")
		}
	})
}

func TestArrayCodecEncodeMultipleDimensions(t *testing.T) {
	skipCockroachDB(t, "Server does not support nested arrays (https://github.com/cockroachdb/cockroach/issues/36815)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select $1::int4[]`, [][]int32{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}})
		require.NoError(t, err)

		for rows.Next() {
			var ss [][]int32
			err := rows.Scan(&ss)
			require.NoError(t, err)
			require.Equal(t, [][]int32{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}, ss)
		}

		require.NoError(t, rows.Err())
	})
}

func TestArrayCodecEncodeMultipleDimensionsRagged(t *testing.T) {
	skipCockroachDB(t, "Server does not support nested arrays (https://github.com/cockroachdb/cockroach/issues/36815)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		rows, err := conn.Query(ctx, `select $1::int4[]`, [][]int32{{1, 2, 3, 4}, {5}, {9, 10, 11, 12}})
		require.Error(t, err, "cannot convert [][]int32 to ArrayGetter because it is a ragged multi-dimensional")
		defer rows.Close()
	})
}

// https://github.com/jackc/pgx/issues/1494
func TestArrayCodecDecodeTextArrayWithTextOfNULL(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		{
			var actual []string
			err := conn.QueryRow(ctx, `select '{"foo", "NULL", " NULL "}'::text[]`).Scan(&actual)
			require.NoError(t, err)
			require.Equal(t, []string{"foo", "NULL", " NULL "}, actual)
		}

		{
			var actual []pgtype.Text
			err := conn.QueryRow(ctx, `select '{"foo", "NULL", NULL, " NULL "}'::text[]`).Scan(&actual)
			require.NoError(t, err)
			require.Equal(t, []pgtype.Text{
				{String: "foo", Valid: true},
				{String: "NULL", Valid: true},
				{},
				{String: " NULL ", Valid: true},
			}, actual)
		}
	})
}

func TestArrayCodecDecodeTextArrayPrefersBinaryFormat(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		sd, err := conn.Prepare(ctx, "", `select '{"foo", "NULL", " NULL "}'::text[]`)
		require.NoError(t, err)
		require.Equal(t, int16(1), conn.TypeMap().FormatCodeForOID(sd.Fields[0].DataTypeOID))
	})
}
