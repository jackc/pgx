package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestRangeCodecTranscode(t *testing.T) {
	testutil.RunTranscodeTests(t, "int4range", []testutil.TranscodeTestCase{
		{
			pgtype.Int4range{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
			new(pgtype.Int4range),
			isExpectedEq(pgtype.Int4range{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true}),
		},
		{
			pgtype.Int4range{
				LowerType: pgtype.Inclusive,
				Lower:     pgtype.Int4{Int: 1, Valid: true},
				Upper:     pgtype.Int4{Int: 5, Valid: true},
				UpperType: pgtype.Exclusive, Valid: true,
			},
			new(pgtype.Int4range),
			isExpectedEq(pgtype.Int4range{
				LowerType: pgtype.Inclusive,
				Lower:     pgtype.Int4{Int: 1, Valid: true},
				Upper:     pgtype.Int4{Int: 5, Valid: true},
				UpperType: pgtype.Exclusive, Valid: true,
			}),
		},
		{pgtype.Int4range{}, new(pgtype.Int4range), isExpectedEq(pgtype.Int4range{})},
		{nil, new(pgtype.Int4range), isExpectedEq(pgtype.Int4range{})},
	})
}

func TestRangeCodecDecodeValue(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	for _, tt := range []struct {
		sql      string
		expected interface{}
	}{
		{
			sql: `select '[1,5)'::int4range`,
			expected: pgtype.GenericRange{
				Lower:     int32(1),
				Upper:     int32(5),
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
		},
	} {
		t.Run(tt.sql, func(t *testing.T) {
			rows, err := conn.Query(context.Background(), tt.sql)
			require.NoError(t, err)

			for rows.Next() {
				values, err := rows.Values()
				require.NoError(t, err)
				require.Len(t, values, 1)
				require.Equal(t, tt.expected, values[0])
			}

			require.NoError(t, rows.Err())
		})
	}
}
