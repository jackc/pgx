package pgtype_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func isExpectedEqBytes(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		ab := a.([]byte)
		vb := v.([]byte)

		if (ab == nil) != (vb == nil) {
			return false
		}

		if ab == nil {
			return true
		}

		return bytes.Compare(ab, vb) == 0
	}
}

func TestByteaCodec(t *testing.T) {
	testPgxCodec(t, "bytea", []PgxTranscodeTestCase{
		{[]byte{1, 2, 3}, new([]byte), isExpectedEqBytes([]byte{1, 2, 3})},
		{[]byte{}, new([]byte), isExpectedEqBytes([]byte{})},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})
}

func TestDriverBytes(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	ctx := context.Background()

	var buf []byte
	err := conn.QueryRow(ctx, `select $1::bytea`, []byte{1, 2}).Scan((*pgtype.DriverBytes)(&buf))
	require.NoError(t, err)

	require.Len(t, buf, 2)
	require.Equal(t, buf, []byte{1, 2})
	require.Equalf(t, cap(buf), len(buf), "cap(buf) is larger than len(buf)")

	// Don't actually have any way to be sure that the bytes are from the driver at the moment as underlying driver
	// doesn't reuse buffers at the present.
}

func TestPreallocBytes(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	ctx := context.Background()

	origBuf := []byte{5, 6, 7, 8}
	buf := origBuf
	err := conn.QueryRow(ctx, `select $1::bytea`, []byte{1, 2}).Scan((*pgtype.PreallocBytes)(&buf))
	require.NoError(t, err)

	require.Len(t, buf, 2)
	require.Equal(t, 4, cap(buf))
	require.Equal(t, buf, []byte{1, 2})

	require.Equal(t, []byte{1, 2, 7, 8}, origBuf)

	err = conn.QueryRow(ctx, `select $1::bytea`, []byte{3, 4, 5, 6, 7}).Scan((*pgtype.PreallocBytes)(&buf))
	require.NoError(t, err)
	require.Len(t, buf, 5)
	require.Equal(t, 5, cap(buf))

	require.Equal(t, []byte{1, 2, 7, 8}, origBuf)
}

func TestUndecodedBytes(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	ctx := context.Background()

	var buf []byte
	err := conn.QueryRow(ctx, `select 1`).Scan((*pgtype.UndecodedBytes)(&buf))
	require.NoError(t, err)

	require.Len(t, buf, 4)
	require.Equal(t, buf, []byte{0, 0, 0, 1})
}
