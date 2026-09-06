package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestRangeCodecTranscode(t *testing.T) {
	skipCockroachDB(t, "Server does not support range types (see https://github.com/cockroachdb/cockroach/issues/27791)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int4range", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Range[pgtype.Int4]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
			Result: new(pgtype.Range[pgtype.Int4]),
			Test:   isExpectedEq(pgtype.Range[pgtype.Int4]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true}),
		},
		{
			Param: pgtype.Range[pgtype.Int4]{
				LowerType: pgtype.Inclusive,
				Lower:     pgtype.Int4{Int32: 1, Valid: true},
				Upper:     pgtype.Int4{Int32: 5, Valid: true},
				UpperType: pgtype.Exclusive, Valid: true,
			},
			Result: new(pgtype.Range[pgtype.Int4]),
			Test: isExpectedEq(pgtype.Range[pgtype.Int4]{
				LowerType: pgtype.Inclusive,
				Lower:     pgtype.Int4{Int32: 1, Valid: true},
				Upper:     pgtype.Int4{Int32: 5, Valid: true},
				UpperType: pgtype.Exclusive, Valid: true,
			}),
		},
		{Param: pgtype.Range[pgtype.Int4]{}, Result: new(pgtype.Range[pgtype.Int4]), Test: isExpectedEq(pgtype.Range[pgtype.Int4]{})},
		{Param: nil, Result: new(pgtype.Range[pgtype.Int4]), Test: isExpectedEq(pgtype.Range[pgtype.Int4]{})},
	})
}

func TestRangeCodecTranscodeCompatibleRangeElementTypes(t *testing.T) {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		pgxtest.SkipCockroachDB(t, conn, "Server does not support range types (see https://github.com/cockroachdb/cockroach/issues/27791)")
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, nil, "numrange", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Range[pgtype.Float8]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
			Result: new(pgtype.Range[pgtype.Float8]),
			Test:   isExpectedEq(pgtype.Range[pgtype.Float8]{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true}),
		},
		{
			Param: pgtype.Range[pgtype.Float8]{
				LowerType: pgtype.Inclusive,
				Lower:     pgtype.Float8{Float64: 1, Valid: true},
				Upper:     pgtype.Float8{Float64: 5, Valid: true},
				UpperType: pgtype.Exclusive, Valid: true,
			},
			Result: new(pgtype.Range[pgtype.Float8]),
			Test: isExpectedEq(pgtype.Range[pgtype.Float8]{
				LowerType: pgtype.Inclusive,
				Lower:     pgtype.Float8{Float64: 1, Valid: true},
				Upper:     pgtype.Float8{Float64: 5, Valid: true},
				UpperType: pgtype.Exclusive, Valid: true,
			}),
		},
		{Param: pgtype.Range[pgtype.Float8]{}, Result: new(pgtype.Range[pgtype.Float8]), Test: isExpectedEq(pgtype.Range[pgtype.Float8]{})},
		{Param: nil, Result: new(pgtype.Range[pgtype.Float8]), Test: isExpectedEq(pgtype.Range[pgtype.Float8]{})},
	})
}

func TestRangeCodecScanRangeTwiceWithUnbounded(t *testing.T) {
	skipCockroachDB(t, "Server does not support range types (see https://github.com/cockroachdb/cockroach/issues/27791)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var r pgtype.Range[pgtype.Int4]

		err := conn.QueryRow(context.Background(), `select '[1,5)'::int4range`).Scan(&r)
		require.NoError(t, err)

		require.Equal(
			t,
			pgtype.Range[pgtype.Int4]{
				Lower:     pgtype.Int4{Int32: 1, Valid: true},
				Upper:     pgtype.Int4{Int32: 5, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			r,
		)

		err = conn.QueryRow(ctx, `select '[1,)'::int4range`).Scan(&r)
		require.NoError(t, err)

		require.Equal(
			t,
			pgtype.Range[pgtype.Int4]{
				Lower:     pgtype.Int4{Int32: 1, Valid: true},
				Upper:     pgtype.Int4{},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Unbounded,
				Valid:     true,
			},
			r,
		)

		err = conn.QueryRow(ctx, `select 'empty'::int4range`).Scan(&r)
		require.NoError(t, err)

		require.Equal(
			t,
			pgtype.Range[pgtype.Int4]{
				Lower:     pgtype.Int4{},
				Upper:     pgtype.Int4{},
				LowerType: pgtype.Empty,
				UpperType: pgtype.Empty,
				Valid:     true,
			},
			r,
		)
	})
}

func TestRangeCodecDecodeValue(t *testing.T) {
	skipCockroachDB(t, "Server does not support range types (see https://github.com/cockroachdb/cockroach/issues/27791)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {
		for _, tt := range []struct {
			sql      string
			expected any
		}{
			{
				sql: `select '[1,5)'::int4range`,
				expected: pgtype.Range[any]{
					Lower:     int32(1),
					Upper:     int32(5),
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Valid:     true,
				},
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

func TestRangeCodecTextBounds(t *testing.T) {
	skipCockroachDB(t, "Server does not support range types")
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create type pg_temp.text_bounds_range as range (subtype = text, collation = "C")`)
		require.NoError(t, err)
		dt, err := conn.LoadType(ctx, "pg_temp.text_bounds_range")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		for _, bound := range []string{"", "a", "a,b", `a"b`, `a\b`, "a(b", "a)b", "a[b", "a]b", "a b", "a\tb", "a\nb", "a\rb", "a\vb", "a\fb", "a{b", "a}b"} {
			for _, lower := range []bool{true, false} {
				input := pgtype.Range[string]{Lower: bound, Upper: bound, LowerType: pgtype.Inclusive, UpperType: pgtype.Inclusive, Valid: true}
				if lower {
					input.UpperType = pgtype.Unbounded
				} else {
					input.LowerType = pgtype.Unbounded
				}
				encoded, err := conn.TypeMap().Encode(dt.OID, pgtype.TextFormatCode, input, []byte("prefix:"))
				require.NoError(t, err)
				require.Equal(t, "prefix:", string(encoded[:7]))

				var actual string
				var infinite bool
				query := `select lower(r), lower_inf(r) from (select $1::text::pg_temp.text_bounds_range r) s`
				if !lower {
					query = `select upper(r), upper_inf(r) from (select $1::text::pg_temp.text_bounds_range r) s`
				}
				err = conn.QueryRow(ctx, query, string(encoded[7:])).Scan(&actual, &infinite)
				if err != nil {
					t.Errorf("bound %q lower=%v encoded=%q: %v", bound, lower, encoded[7:], err)
					continue
				}
				require.False(t, infinite, "bound %q lower=%v", bound, lower)
				require.Equal(t, bound, actual, "lower=%v", lower)
			}
		}
	})
}

func TestRangeCodecTextFiniteBounds(t *testing.T) {
	skipCockroachDB(t, "Server does not support range types")
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `create type pg_temp.text_finite_bounds_range as range (subtype = text, collation = "C")`)
		require.NoError(t, err)
		dt, err := conn.LoadType(ctx, "pg_temp.text_finite_bounds_range")
		require.NoError(t, err)
		conn.TypeMap().RegisterType(dt)

		input := pgtype.Range[string]{Lower: `a"\,([`, Upper: `z"\,)]`, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}
		for _, capacity := range []int{7, 256} {
			buf := make([]byte, 7, capacity)
			copy(buf, "prefix:")
			encoded, err := conn.TypeMap().Encode(dt.OID, pgtype.TextFormatCode, input, buf)
			require.NoError(t, err)
			require.Equal(t, "prefix:", string(encoded[:7]))
			var lower, upper string
			var lowerInclusive, upperInclusive bool
			err = conn.QueryRow(ctx, `select lower(r), upper(r), lower_inc(r), upper_inc(r) from (select $1::text::pg_temp.text_finite_bounds_range r) s`, string(encoded[7:])).Scan(&lower, &upper, &lowerInclusive, &upperInclusive)
			require.NoError(t, err, "capacity=%d encoded=%q", capacity, encoded[7:])
			require.Equal(t, input.Lower, lower)
			require.Equal(t, input.Upper, upper)
			require.True(t, lowerInclusive)
			require.False(t, upperInclusive)
		}
	})
}
