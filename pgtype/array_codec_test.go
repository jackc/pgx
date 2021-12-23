package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/assert"
)

func TestArrayCodec(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	tests := []struct {
		expected []int16
	}{
		{[]int16(nil)},
		{[]int16{}},
		{[]int16{1, 2, 3}},
	}
	for i, tt := range tests {
		var actual []int16
		err := conn.QueryRow(
			context.Background(),
			"select $1::smallint[]",
			tt.expected,
		).Scan(&actual)
		assert.NoErrorf(t, err, "%d", i)
		assert.Equalf(t, tt.expected, actual, "%d", i)
	}
}

// func TestArrayCodecValue(t *testing.T) {
// 	ArrayCodec := pgtype.NewArrayCodec("_text", pgtype.TextOID, func() pgtype.ValueTranscoder { return &pgtype.Text{} })

// 	err := ArrayCodec.Set(nil)
// 	require.NoError(t, err)

// 	gotValue := ArrayCodec.Get()
// 	require.Nil(t, gotValue)

// 	slice := []string{"foo", "bar"}
// 	err = ArrayCodec.AssignTo(&slice)
// 	require.NoError(t, err)
// 	require.Nil(t, slice)

// 	err = ArrayCodec.Set([]string{})
// 	require.NoError(t, err)

// 	gotValue = ArrayCodec.Get()
// 	require.Len(t, gotValue, 0)

// 	err = ArrayCodec.AssignTo(&slice)
// 	require.NoError(t, err)
// 	require.EqualValues(t, []string{}, slice)

// 	err = ArrayCodec.Set([]string{"baz", "quz"})
// 	require.NoError(t, err)

// 	gotValue = ArrayCodec.Get()
// 	require.Len(t, gotValue, 2)

// 	err = ArrayCodec.AssignTo(&slice)
// 	require.NoError(t, err)
// 	require.EqualValues(t, []string{"baz", "quz"}, slice)
// }

// func TestArrayCodecTranscode(t *testing.T) {
// 	conn := testutil.MustConnectPgx(t)
// 	defer testutil.MustCloseContext(t, conn)

// 	conn.ConnInfo().RegisterDataType(pgtype.DataType{
// 		Value: pgtype.NewArrayCodec("_text", pgtype.TextOID, func() pgtype.ValueTranscoder { return &pgtype.Text{} }),
// 		Name:  "_text",
// 		OID:   pgtype.TextArrayOID,
// 	})

// 	var dstStrings []string
// 	err := conn.QueryRow(context.Background(), "select $1::text[]", []string{"red", "green", "blue"}).Scan(&dstStrings)
// 	require.NoError(t, err)

// 	require.EqualValues(t, []string{"red", "green", "blue"}, dstStrings)
// }

// func TestArrayCodecEmptyArrayDoesNotBreakArrayCodec(t *testing.T) {
// 	conn := testutil.MustConnectPgx(t)
// 	defer testutil.MustCloseContext(t, conn)

// 	conn.ConnInfo().RegisterDataType(pgtype.DataType{
// 		Value: pgtype.NewArrayCodec("_text", pgtype.TextOID, func() pgtype.ValueTranscoder { return &pgtype.Text{} }),
// 		Name:  "_text",
// 		OID:   pgtype.TextArrayOID,
// 	})

// 	var dstStrings []string
// 	err := conn.QueryRow(context.Background(), "select '{}'::text[]").Scan(&dstStrings)
// 	require.NoError(t, err)

// 	require.EqualValues(t, []string{}, dstStrings)

// 	err = conn.QueryRow(context.Background(), "select $1::text[]", []string{"red", "green", "blue"}).Scan(&dstStrings)
// 	require.NoError(t, err)

// 	require.EqualValues(t, []string{"red", "green", "blue"}, dstStrings)
// }
