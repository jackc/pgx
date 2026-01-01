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
			Param:  pgtype.Uint64{Uint64: 1 << 36, Valid: true},
			Result: new(pgtype.Uint64),
			Test:   isExpectedEq(pgtype.Uint64{Uint64: 1 << 36, Valid: true}),
		},
		{Param: pgtype.Uint64{}, Result: new(pgtype.Uint64), Test: isExpectedEq(pgtype.Uint64{})},
		{Param: nil, Result: new(pgtype.Uint64), Test: isExpectedEq(pgtype.Uint64{})},
		{
			Param:  uint64(1 << 36),
			Result: new(uint64),
			Test:   isExpectedEq(uint64(1 << 36)),
		},
		{Param: "1147", Result: new(string), Test: isExpectedEq("1147")},
	})
}
