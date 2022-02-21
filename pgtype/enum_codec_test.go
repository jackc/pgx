package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestEnumCodec(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists enum_test;

create type enum_test as enum ('foo', 'bar', 'baz');`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type enum_test")

	dt, err := conn.LoadType(context.Background(), "enum_test")
	require.NoError(t, err)

	conn.TypeMap().RegisterType(dt)

	var s string
	err = conn.QueryRow(context.Background(), `select 'foo'::enum_test`).Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "foo", s)

	err = conn.QueryRow(context.Background(), `select $1::enum_test`, "bar").Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "bar", s)

	err = conn.QueryRow(context.Background(), `select 'foo'::enum_test`).Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "foo", s)

	err = conn.QueryRow(context.Background(), `select $1::enum_test`, "bar").Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "bar", s)

	err = conn.QueryRow(context.Background(), `select 'baz'::enum_test`).Scan(&s)
	require.NoError(t, err)
	require.Equal(t, "baz", s)
}

func TestEnumCodecValues(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists enum_test;

create type enum_test as enum ('foo', 'bar', 'baz');`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type enum_test")

	dt, err := conn.LoadType(context.Background(), "enum_test")
	require.NoError(t, err)

	conn.TypeMap().RegisterType(dt)

	rows, err := conn.Query(context.Background(), `select 'foo'::enum_test`)
	require.NoError(t, err)
	require.True(t, rows.Next())
	values, err := rows.Values()
	require.NoError(t, err)
	require.Equal(t, values, []interface{}{"foo"})
}
