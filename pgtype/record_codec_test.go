package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestRecordCodec(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	var a string
	var b int32
	err := conn.QueryRow(context.Background(), `select row('foo'::text, 42::int4)`).Scan(pgtype.CompositeFields{&a, &b})
	require.NoError(t, err)

	require.Equal(t, "foo", a)
	require.Equal(t, int32(42), b)
}

func TestRecordCodecDecodeValue(t *testing.T) {
	skipCockroachDB(t, "Server converts row int4 to int8")

	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	for _, tt := range []struct {
		sql      string
		expected interface{}
	}{
		{
			sql:      `select row()`,
			expected: []interface{}{},
		},
		{
			sql:      `select row('foo'::text, 42::int4)`,
			expected: []interface{}{"foo", int32(42)},
		},
		{
			sql:      `select row(100.0::float4, 1.09::float4)`,
			expected: []interface{}{float32(100), float32(1.09)},
		},
		{
			sql:      `select row('foo'::text, array[1, 2, null, 4]::int4[], 42::int4)`,
			expected: []interface{}{"foo", []interface{}{int32(1), int32(2), nil, int32(4)}, int32(42)},
		},
		{
			sql:      `select row(null)`,
			expected: []interface{}{nil},
		},
		{
			sql:      `select null::record`,
			expected: nil,
		},
	} {
		t.Run(tt.sql, func(t *testing.T) {
			rows, err := conn.Query(context.Background(), tt.sql)
			require.NoError(t, err)
			defer rows.Close()

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
