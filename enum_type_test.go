package pgtype_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupEnum(t *testing.T, conn *pgx.Conn) *pgtype.EnumType {
	_, err := conn.Exec(context.Background(), "drop type if exists pgtype_enum_color;")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), "create type pgtype_enum_color as enum ('blue', 'green', 'purple');")
	require.NoError(t, err)

	var oid uint32
	err = conn.QueryRow(context.Background(), "select oid from pg_type where typname=$1;", "pgtype_enum_color").Scan(&oid)
	require.NoError(t, err)

	et := pgtype.NewEnumType("pgtype_enum_color", []string{"blue", "green", "purple"})
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: et, Name: "pgtype_enum_color", OID: oid})

	return et
}

func cleanupEnum(t *testing.T, conn *pgx.Conn) {
	_, err := conn.Exec(context.Background(), "drop type if exists pgtype_enum_color;")
	require.NoError(t, err)
}

func TestEnumTypeTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	setupEnum(t, conn)
	defer cleanupEnum(t, conn)

	var dst string
	err := conn.QueryRow(context.Background(), "select $1::pgtype_enum_color", "blue").Scan(&dst)
	require.NoError(t, err)
	require.EqualValues(t, "blue", dst)
}

func TestEnumTypeSet(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	enumType := setupEnum(t, conn)
	defer cleanupEnum(t, conn)

	successfulTests := []struct {
		source interface{}
		result interface{}
	}{
		{source: "blue", result: "blue"},
		{source: _string("green"), result: "green"},
		{source: (*string)(nil), result: nil},
	}

	for i, tt := range successfulTests {
		err := enumType.Set(tt.source)
		assert.NoErrorf(t, err, "%d", i)
		assert.Equalf(t, tt.result, enumType.Get(), "%d", i)
	}
}

func TestEnumTypeAssignTo(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	enumType := setupEnum(t, conn)
	defer cleanupEnum(t, conn)

	{
		var s string

		err := enumType.Set("blue")
		require.NoError(t, err)

		err = enumType.AssignTo(&s)
		require.NoError(t, err)

		assert.EqualValues(t, "blue", s)
	}

	{
		var ps *string

		err := enumType.Set("blue")
		require.NoError(t, err)

		err = enumType.AssignTo(&ps)
		require.NoError(t, err)

		assert.EqualValues(t, "blue", *ps)
	}

	{
		var ps *string

		err := enumType.Set(nil)
		require.NoError(t, err)

		err = enumType.AssignTo(&ps)
		require.NoError(t, err)

		assert.EqualValues(t, (*string)(nil), ps)
	}

	var buf []byte
	bytesTests := []struct {
		src      interface{}
		dst      *[]byte
		expected []byte
	}{
		{src: "blue", dst: &buf, expected: []byte("blue")},
		{src: nil, dst: &buf, expected: nil},
	}

	for i, tt := range bytesTests {
		err := enumType.Set(tt.src)
		require.NoError(t, err, "%d", i)

		err = enumType.AssignTo(tt.dst)
		require.NoError(t, err, "%d", i)

		if bytes.Compare(*tt.dst, tt.expected) != 0 {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, tt.dst)
		}
	}

	{
		var s string

		err := enumType.Set(nil)
		require.NoError(t, err)

		err = enumType.AssignTo(&s)
		require.Error(t, err)
	}

}
