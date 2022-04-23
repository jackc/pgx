package pgtype_test

import (
	"context"
	"reflect"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/require"
)

func TestMultirangeCodecTranscode(t *testing.T) {
	skipPostgreSQLVersionLessThan(t, 14)
	skipCockroachDB(t, "Server does not support range types (see https://github.com/cockroachdb/cockroach/issues/27791)")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int4multirange", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Multirange[pgtype.Range[pgtype.Int4]](nil),
			new(pgtype.Multirange[pgtype.Range[pgtype.Int4]]),
			func(a any) bool { return reflect.DeepEqual(pgtype.Multirange[pgtype.Range[pgtype.Int4]](nil), a) },
		},
		{
			pgtype.Multirange[pgtype.Range[pgtype.Int4]]{},
			new(pgtype.Multirange[pgtype.Range[pgtype.Int4]]),
			func(a any) bool { return reflect.DeepEqual(pgtype.Multirange[pgtype.Range[pgtype.Int4]]{}, a) },
		},
		{
			pgtype.Multirange[pgtype.Range[pgtype.Int4]]{
				{
					Lower:     pgtype.Int4{Int32: 1, Valid: true},
					Upper:     pgtype.Int4{Int32: 5, Valid: true},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Valid:     true,
				},
				{
					Lower:     pgtype.Int4{Int32: 7, Valid: true},
					Upper:     pgtype.Int4{Int32: 9, Valid: true},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Valid:     true,
				},
			},
			new(pgtype.Multirange[pgtype.Range[pgtype.Int4]]),
			func(a any) bool {
				return reflect.DeepEqual(pgtype.Multirange[pgtype.Range[pgtype.Int4]]{
					{
						Lower:     pgtype.Int4{Int32: 1, Valid: true},
						Upper:     pgtype.Int4{Int32: 5, Valid: true},
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					},
					{
						Lower:     pgtype.Int4{Int32: 7, Valid: true},
						Upper:     pgtype.Int4{Int32: 9, Valid: true},
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					},
				}, a)
			},
		},
	})
}

func TestMultirangeCodecDecodeValue(t *testing.T) {
	skipPostgreSQLVersionLessThan(t, 14)
	skipCockroachDB(t, "Server does not support range types (see https://github.com/cockroachdb/cockroach/issues/27791)")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, _ testing.TB, conn *pgx.Conn) {

		for _, tt := range []struct {
			sql      string
			expected any
		}{
			{
				sql: `select int4multirange(int4range(1, 5), int4range(7,9))`,
				expected: pgtype.Multirange[pgtype.Range[any]]{
					{
						Lower:     int32(1),
						Upper:     int32(5),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					},
					{
						Lower:     int32(7),
						Upper:     int32(9),
						LowerType: pgtype.Inclusive,
						UpperType: pgtype.Exclusive,
						Valid:     true,
					},
				},
			},
		} {
			t.Run(tt.sql, func(t *testing.T) {
				rows, err := conn.Query(ctx, tt.sql)
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
	})
}
