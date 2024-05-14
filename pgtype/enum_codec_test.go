package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestEnumCodec(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop type if exists enum_test;

create type enum_test as enum ('foo', 'bar', 'baz');`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type enum_test")

		dt, err := conn.LoadType(ctx, "enum_test")
		require.NoError(t, err)

		conn.TypeMap().RegisterType(dt)

		var s string
		err = conn.QueryRow(ctx, `select 'foo'::enum_test`).Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "foo", s)

		err = conn.QueryRow(ctx, `select $1::enum_test`, "bar").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "bar", s)

		err = conn.QueryRow(ctx, `select 'foo'::enum_test`).Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "foo", s)

		err = conn.QueryRow(ctx, `select $1::enum_test`, "bar").Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "bar", s)

		err = conn.QueryRow(ctx, `select 'baz'::enum_test`).Scan(&s)
		require.NoError(t, err)
		require.Equal(t, "baz", s)
	})
}

func TestEnumCodecValues(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {

		_, err := conn.Exec(ctx, `drop type if exists enum_test;

create type enum_test as enum ('foo', 'bar', 'baz');`)
		require.NoError(t, err)
		defer conn.Exec(ctx, "drop type enum_test")

		dt, err := conn.LoadType(ctx, "enum_test")
		require.NoError(t, err)

		conn.TypeMap().RegisterType(dt)

		rows, err := conn.Query(ctx, `select 'foo'::enum_test`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		values, err := rows.Values()
		require.NoError(t, err)
		require.Equal(t, []any{"foo"}, values)
	})
}
