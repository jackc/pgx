package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestPgLSNCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support pg_lsn type")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "pg_lsn", []pgxtest.ValueRoundTripTest{
		{
			pgtype.PgLSN{LSN: pgtype.TextOID, Valid: true},
			new(pgtype.PgLSN),
			isExpectedEq(pgtype.PgLSN{LSN: pgtype.TextOID, Valid: true}),
		},
		{pgtype.PgLSN{}, new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{})},
		{nil, new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{})},
	})
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "text", []pgxtest.ValueRoundTripTest{
		{
			pgtype.PgLSN{LSN: pgtype.TextOID, Valid: true},
			new(pgtype.PgLSN),
			isExpectedEq(pgtype.PgLSN{LSN: pgtype.TextOID, Valid: true}),
		},
		{pgtype.PgLSN{}, new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{})},
		{nil, new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{})},
	})
}
