package pgtype_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgx/v4"
)

var recordArrayTests = []struct {
	sql      string
	expected pgtype.RecordArray
}{
	{
		sql: `select array_agg((x::int4, x+100::int8)) from generate_series(0, 1) x;`,
		expected: pgtype.RecordArray{
			Dimensions: []pgtype.ArrayDimension{
				{LowerBound: 1, Length: 2},
			},
			Elements: []pgtype.Record{
				{
					Fields: []pgtype.Value{
						&pgtype.Int4{Int: 0, Status: pgtype.Present},
						&pgtype.Int8{Int: 100, Status: pgtype.Present},
					},
					Status: pgtype.Present,
				},
				{
					Fields: []pgtype.Value{
						&pgtype.Int4{Int: 1, Status: pgtype.Present},
						&pgtype.Int8{Int: 101, Status: pgtype.Present},
					},
					Status: pgtype.Present,
				},
			},
			Status: pgtype.Present,
		},
	},
}

func TestRecordArrayTranscode(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	for i, tt := range recordArrayTests {
		psName := fmt.Sprintf("test%d", i)
		_, err := conn.Prepare(context.Background(), psName, tt.sql)
		require.NoError(t, err)

		t.Run(tt.sql, func(t *testing.T) {
			var result pgtype.RecordArray
			err := conn.QueryRow(context.Background(), psName, pgx.QueryResultFormats{pgx.BinaryFormatCode}).Scan(&result)
			require.NoError(t, err)

			require.Equal(t, tt.expected, result)
		})

	}
}

func TestRecordArrayAssignTo(t *testing.T) {
	src := pgtype.RecordArray{
		Dimensions: []pgtype.ArrayDimension{
			{LowerBound: 1, Length: 2},
		},
		Elements: []pgtype.Record{
			{
				Fields: []pgtype.Value{
					&pgtype.Int4{Int: 0, Status: pgtype.Present},
					&pgtype.Int8{Int: 100, Status: pgtype.Present},
				},
				Status: pgtype.Present,
			},
			{
				Fields: []pgtype.Value{
					&pgtype.Int4{Int: 1, Status: pgtype.Present},
					&pgtype.Int8{Int: 101, Status: pgtype.Present},
				},
				Status: pgtype.Present,
			},
		},
		Status: pgtype.Present,
	}
	dst := [][]pgtype.Value{}
	err := src.AssignTo(&dst)
	require.NoError(t, err)

	expected := [][]pgtype.Value{
		{
			&pgtype.Int4{Int: 0, Status: pgtype.Present},
			&pgtype.Int8{Int: 100, Status: pgtype.Present},
		},
		{
			&pgtype.Int4{Int: 1, Status: pgtype.Present},
			&pgtype.Int8{Int: 101, Status: pgtype.Present},
		},
	}
	require.Equal(t, expected, dst)
}
