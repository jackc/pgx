package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestArrayTypeValue(t *testing.T) {
	arrayType := pgtype.NewArrayType("_text", pgtype.TextOID, func() pgtype.ValueTranscoder { return &pgtype.Text{} })

	err := arrayType.Set(nil)
	require.NoError(t, err)

	gotValue := arrayType.Get()
	require.Nil(t, gotValue)

	slice := []string{"foo", "bar"}
	err = arrayType.AssignTo(&slice)
	require.NoError(t, err)
	require.Nil(t, slice)

	err = arrayType.Set([]string{})
	require.NoError(t, err)

	gotValue = arrayType.Get()
	require.Len(t, gotValue, 0)

	err = arrayType.AssignTo(&slice)
	require.NoError(t, err)
	require.EqualValues(t, []string{}, slice)

	err = arrayType.Set([]string{"baz", "quz"})
	require.NoError(t, err)

	gotValue = arrayType.Get()
	require.Len(t, gotValue, 2)

	err = arrayType.AssignTo(&slice)
	require.NoError(t, err)
	require.EqualValues(t, []string{"baz", "quz"}, slice)
}

func TestArrayTypeTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	conn.ConnInfo().RegisterDataType(pgtype.DataType{
		Value: pgtype.NewArrayType("_text", pgtype.TextOID, func() pgtype.ValueTranscoder { return &pgtype.Text{} }),
		Name:  "_text",
		OID:   pgtype.TextArrayOID,
	})

	var dstStrings []string
	err := conn.QueryRow(context.Background(), "select $1::text[]", []string{"red", "green", "blue"}).Scan(&dstStrings)
	require.NoError(t, err)

	require.EqualValues(t, []string{"red", "green", "blue"}, dstStrings)
}

func TestArrayTypeEmptyArrayDoesNotBreakArrayType(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	conn.ConnInfo().RegisterDataType(pgtype.DataType{
		Value: pgtype.NewArrayType("_text", pgtype.TextOID, func() pgtype.ValueTranscoder { return &pgtype.Text{} }),
		Name:  "_text",
		OID:   pgtype.TextArrayOID,
	})

	var dstStrings []string
	err := conn.QueryRow(context.Background(), "select '{}'::text[]").Scan(&dstStrings)
	require.NoError(t, err)

	require.EqualValues(t, []string{}, dstStrings)

	err = conn.QueryRow(context.Background(), "select $1::text[]", []string{"red", "green", "blue"}).Scan(&dstStrings)
	require.NoError(t, err)

	require.EqualValues(t, []string{"red", "green", "blue"}, dstStrings)
}
