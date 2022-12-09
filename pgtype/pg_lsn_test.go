package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestPgLSNCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support pg_lsn type")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "pg_lsn", []pgxtest.ValueRoundTripTest{
		{nil, new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{LSN: 0, Valid: false})},
		{[]byte(nil), new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{LSN: 0, Valid: false})},
		{nil, new([]byte), isExpectedEqBytes([]byte(nil))},
		{[]byte(nil), new([]byte), isExpectedEqBytes([]byte(nil))},
		{nil, new(*string), isExpectedEq((*string)(nil))},
		{[]byte(nil), new(*string), isExpectedEq((*string)(nil))},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "pg_lsn", []pgxtest.ValueRoundTripTest{
		{[]byte("1F/FFFFFFFF"), new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{LSN: 0x1F_FFFFFFFF, Valid: true})},
		{[]byte("1F/FFFFFFFF"), new([]byte), isExpectedEqBytes([]byte("1F/FFFFFFFF"))},
		{[]byte("1F/FFFFFFFF"), new(string), isExpectedEq("1F/FFFFFFFF")},
		{[]byte("FFFFFFFF/FFFFFFFF"), new(pgtype.PgLSN), isExpectedEq(pgtype.PgLSN{LSN: 0xFFFFFFFF_FFFFFFFF, Valid: true})},
		{[]byte("FFFFFFFF/FFFFFFFF"), new([]byte), isExpectedEqBytes([]byte("FFFFFFFF/FFFFFFFF"))},
		{[]byte("FFFFFFFF/FFFFFFFF"), new(string), isExpectedEq("FFFFFFFF/FFFFFFFF")},
	})
}
