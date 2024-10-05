package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestUint64Codec(t *testing.T) {
	skipCockroachDB(t, "Server does not support xid8 (https://github.com/cockroachdb/cockroach/issues/36815)")
	skipPostgreSQLVersionLessThan(t, 13)

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "xid8", []pgxtest.ValueRoundTripTest{
		{
			pgtype.Uint64{Uint64: 1 << 36, Valid: true},
			new(pgtype.Uint64),
			isExpectedEq(pgtype.Uint64{Uint64: 1 << 36, Valid: true}),
		},
		{pgtype.Uint64{}, new(pgtype.Uint64), isExpectedEq(pgtype.Uint64{})},
		{nil, new(pgtype.Uint64), isExpectedEq(pgtype.Uint64{})},
		{
			uint64(1 << 36),
			new(uint64),
			isExpectedEq(uint64(1 << 36)),
		},
		{"1147", new(string), isExpectedEq("1147")},
	})
}
