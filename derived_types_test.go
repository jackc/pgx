package pgx_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestLoadTypesSharedDependencies(t *testing.T) {
	skipCockroachDB(t, "Server does not support composite types")

	pgxtest.RunWithQueryExecModes(context.Background(), t, defaultConnTestRunner, nil, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = conn.Exec(ctx, `
create domain loadtypes_shared_amount as int4;
create type loadtypes_shared_status as enum ('pending', 'paid');
create type loadtypes_shared_line as (amount loadtypes_shared_amount, status loadtypes_shared_status);
create type loadtypes_shared_bundle as (
    first loadtypes_shared_line,
    second loadtypes_shared_line,
    lines loadtypes_shared_line[]
);
create type loadtypes_shared_order as (
    direct loadtypes_shared_line,
    bundle loadtypes_shared_bundle,
    bundles loadtypes_shared_bundle[]
);`)
		require.NoError(t, err)

		// The line type is reachable through several paths of different lengths.
		// Its dependencies must be registered before every type that uses it.
		types, err := conn.LoadTypes(ctx, []string{"loadtypes_shared_order", "public.loadtypes_shared_order"})
		require.NoError(t, err)
		var names []string
		for _, typ := range types {
			names = append(names, typ.Name)
		}
		var expectedNames []string
		for _, name := range []string{"amount", "status", "line", "bundle", "order"} {
			expectedNames = append(expectedNames, "loadtypes_shared_"+name, "public.loadtypes_shared_"+name)
		}
		for _, name := range []string{"line", "bundle"} {
			expectedNames = append(expectedNames, "_loadtypes_shared_"+name, "public._loadtypes_shared_"+name)
		}
		require.ElementsMatch(t, expectedNames, names)

		type line struct {
			Amount int32
			Status string
		}
		type bundle struct {
			First  line
			Second line
			Lines  []line
		}
		type order struct {
			Direct  line
			Bundle  bundle
			Bundles []bundle
		}
		conn.TypeMap().RegisterDefaultPgType(order{}, "loadtypes_shared_order")
		want := order{
			Direct:  line{10, "pending"},
			Bundle:  bundle{line{20, "paid"}, line{30, "pending"}, []line{{40, "paid"}, {50, "pending"}}},
			Bundles: []bundle{{line{60, "paid"}, line{70, "pending"}, []line{{80, "paid"}}}},
		}
		for _, format := range []int16{pgx.TextFormatCode, pgx.BinaryFormatCode} {
			var got order
			err = conn.QueryRow(ctx, "select $1::loadtypes_shared_order", pgx.QueryResultFormats{format}, want).Scan(&got)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	})
}

func BenchmarkLoadTypes(b *testing.B) {
	skipCockroachDB(b, "Server does not support composite types")

	for _, tc := range []struct {
		name  string
		count int
		width int
	}{
		{"Enums53", 53, 0},
		{"Nested4", 4, 4},
		{"Nested8", 8, 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			defaultConnTestRunner.RunTest(context.Background(), b, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(ctx)

				var names []string
				child := "text"
				for i := 0; i < tc.count; i++ {
					name := fmt.Sprintf("loadtypes_bench_%d", i)
					definition := "enum ('a', 'b')"
					if tc.width > 0 {
						var fields []string
						for j := 0; j < tc.width; j++ {
							fields = append(fields, fmt.Sprintf("field_%d %s", j, child))
						}
						definition = "(" + strings.Join(fields, ", ") + ")"
					}
					_, err = conn.Exec(ctx, "create type "+name+" as "+definition)
					require.NoError(t, err)
					names = append(names, name)
					child = name
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := conn.LoadTypes(ctx, names); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		})
	}
}

func TestCompositeCodecTranscodeWithLoadTypes(t *testing.T) {
	skipCockroachDB(t, "Server does not support composite types (see https://github.com/cockroachdb/cockroach/issues/27792)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `
drop type if exists dtype_test;
drop domain if exists anotheruint64;

create domain anotheruint64 as numeric(20,0);
create type dtype_test as (
  a text,
  b int4,
  c anotheruint64,
  d anotheruint64[]
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type dtype_test")
		defer conn.Exec(ctx, "drop domain anotheruint64")

		types, err := conn.LoadTypes(ctx, []string{"dtype_test"})
		require.NoError(t, err)
		require.Len(t, types, 6)
		require.Equal(t, types[0].Name, "public.anotheruint64")
		require.Equal(t, types[1].Name, "anotheruint64")
		require.Equal(t, types[2].Name, "public._anotheruint64")
		require.Equal(t, types[3].Name, "_anotheruint64")
		require.Equal(t, types[4].Name, "public.dtype_test")
		require.Equal(t, types[5].Name, "dtype_test")
	})
}

// https://github.com/jackc/pgx/issues/2608
func TestLoadTypesDoesNotOverwriteBuiltinCodecsForGeometricFields(t *testing.T) {
	skipCockroachDB(t, "Server does not support composite types (see https://github.com/cockroachdb/cockroach/issues/27792)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		_, err := conn.Exec(ctx, `
drop type if exists dtype_geometric_test;

create type dtype_geometric_test as (
  m_id int4,
  m_area box
);`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type dtype_geometric_test")

		types, err := conn.LoadTypes(ctx, []string{"dtype_geometric_test"})
		require.NoError(t, err)

		// box and point are base types that set typelem to describe their internal
		// representation, not to mark themselves as arrays. LoadTypes must not treat
		// them as arrays and must not re-register them, or it clobbers their existing
		// built-in codec with a bogus ArrayCodec (see issue #2608).
		for _, typ := range types {
			require.NotContains(t, []string{"box", "pg_catalog.box", "point", "pg_catalog.point"}, typ.Name)
		}

		conn.TypeMap().RegisterTypes(types)

		var id int32
		area := pgtype.Box{P: [2]pgtype.Vec2{{X: 1, Y: 2}, {X: 3, Y: 4}}, Valid: true}

		err = conn.QueryRow(ctx, "select $1::dtype_geometric_test",
			pgtype.CompositeFields{int32(1), area},
		).Scan(
			pgtype.CompositeFields{&id, &area},
		)
		require.NoError(t, err)
		require.EqualValues(t, 1, id)
		require.True(t, area.Valid)
	})
}

func TestLoadTypesLoadsArrayDelimiter(t *testing.T) {
	skipCockroachDB(t, "Server does not support box type")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		types, err := conn.LoadTypes(ctx, []string{"_box"})
		require.NoError(t, err)

		var found bool
		for _, dt := range types {
			if dt.Name != "_box" && dt.Name != "pg_catalog._box" {
				continue
			}

			codec, ok := dt.Codec.(*pgtype.ArrayCodec)
			require.True(t, ok)
			require.Equal(t, byte(';'), codec.Delimiter)
			found = true
		}

		require.True(t, found)
	})
}
