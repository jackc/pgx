package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestUint64Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "pg_lsn", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Uint64{Uint64: pgtype.TextOID, Valid: true},
			new(pgtype.Uint64),
			isExpectedEq(pgtype.Uint64{Uint64: pgtype.TextOID, Valid: true}),
		},
		{pgtype.Uint64{}, new(pgtype.Uint64), isExpectedEq(pgtype.Uint64{})},
		{nil, new(pgtype.Uint64), isExpectedEq(pgtype.Uint64{})},
	})
}
