package pgtype_test

import (
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestCompositeCodecTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	_, err := conn.Exec(context.Background(), `drop type if exists ct_test;

create type ct_test as (
	a text,
  b int4
);`)
	require.NoError(t, err)
	defer conn.Exec(context.Background(), "drop type ct_test")

	var oid uint32
	err = conn.QueryRow(context.Background(), `select 'ct_test'::regtype::oid`).Scan(&oid)
	require.NoError(t, err)

	defer conn.Exec(context.Background(), "drop type ct_test")

	textDataType, ok := conn.ConnInfo().DataTypeForOID(pgtype.TextOID)
	require.True(t, ok)

	int4DataType, ok := conn.ConnInfo().DataTypeForOID(pgtype.Int4OID)
	require.True(t, ok)

	conn.ConnInfo().RegisterDataType(pgtype.DataType{
		Name: "ct_test",
		OID:  oid,
		Codec: &pgtype.CompositeCodec{
			Fields: []pgtype.CompositeCodecField{
				{
					Name:     "a",
					DataType: textDataType,
				},
				{
					Name:     "b",
					DataType: int4DataType,
				},
			},
		},
	})

	formats := []struct {
		name string
		code int16
	}{
		{name: "TextFormat", code: pgx.TextFormatCode},
		{name: "BinaryFormat", code: pgx.BinaryFormatCode},
	}

	for _, format := range formats {
		var a string
		var b int32

		err := conn.QueryRow(context.Background(), "select $1::ct_test", pgx.QueryResultFormats{format.code},
			pgtype.CompositeFields{"hi", int32(42)},
		).Scan(
			pgtype.CompositeFields{&a, &b},
		)
		require.NoErrorf(t, err, "%v", format.name)
		require.EqualValuesf(t, "hi", a, "%v", format.name)
		require.EqualValuesf(t, 42, b, "%v", format.name)
	}
}
