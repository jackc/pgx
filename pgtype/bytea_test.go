package pgtype_test

import (
	"bytes"
	"context"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func isExpectedEqBytes(a any) func(any) bool {
	return func(v any) bool {
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
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "bytea", []pgxtest.ValueRoundTripTest{
		{[]byte{1, 2, 3}, new([]byte), isExpectedEqBytes([]byte{1, 2, 3})},
		{[]byte{}, new([]byte), isExpectedEqBytes([]byte{})},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
	})
}

func TestDriverBytesQueryRow(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var buf []byte
		err := conn.QueryRow(ctx, `select $1::bytea`, []byte{1, 2}).Scan((*pgtype.DriverBytes)(&buf))
		require.EqualError(t, err, "cannot scan into *pgtype.DriverBytes from QueryRow")
	})
}

func TestDriverBytes(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		argBuf := make([]byte, 128)
		for i := range argBuf {
			argBuf[i] = byte(i)
		}

		rows, err := conn.Query(ctx, `select $1::bytea from generate_series(1, 1000)`, argBuf)
		require.NoError(t, err)
		defer rows.Close()

		rowCount := 0
		resultBuf := argBuf
		detectedResultMutation := false
		for rows.Next() {
			rowCount++

			// At some point the buffer should be reused and change.
			if bytes.Compare(argBuf, resultBuf) != 0 {
				detectedResultMutation = true
			}

			err = rows.Scan((*pgtype.DriverBytes)(&resultBuf))
			require.NoError(t, err)

			require.Len(t, resultBuf, len(argBuf))
			require.Equal(t, resultBuf, argBuf)
			require.Equalf(t, cap(resultBuf), len(resultBuf), "cap(resultBuf) is larger than len(resultBuf)")
		}

		require.True(t, detectedResultMutation)

		err = rows.Err()
		require.NoError(t, err)
	})
}

func TestPreallocBytes(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
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
	})
}

func TestUndecodedBytes(t *testing.T) {
	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var buf []byte
		err := conn.QueryRow(ctx, `select 1::int4`).Scan((*pgtype.UndecodedBytes)(&buf))
		require.NoError(t, err)

		require.Len(t, buf, 4)
		require.Equal(t, buf, []byte{0, 0, 0, 1})
	})
}
