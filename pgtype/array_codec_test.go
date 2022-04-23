package pgtype_test

import (
	"context"
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

func TestArrayCodecFlatArray(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		for i, tt := range []struct {
			expected any
		}{
			{pgtype.FlatArray[int32](nil)},
			{pgtype.FlatArray[int32]{}},
			{pgtype.FlatArray[int32]{1, 2, 3}},
		} {
			var actual pgtype.FlatArray[int32]
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

func TestArrayCodecAnySlice(t *testing.T) {
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
