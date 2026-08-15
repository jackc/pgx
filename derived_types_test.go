package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

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
