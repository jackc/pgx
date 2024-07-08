package pgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
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
