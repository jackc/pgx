package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestUint32Codec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, pgxtest.KnownOIDQueryExecModes, "oid", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Uint32{Uint32: pgtype.TextOID, Valid: true},
			Result: new(pgtype.Uint32),
			Test:   isExpectedEq(pgtype.Uint32{Uint32: pgtype.TextOID, Valid: true}),
		},
		{Param: pgtype.Uint32{}, Result: new(pgtype.Uint32), Test: isExpectedEq(pgtype.Uint32{})},
		{Param: nil, Result: new(pgtype.Uint32), Test: isExpectedEq(pgtype.Uint32{})},
		{Param: "1147", Result: new(string), Test: isExpectedEq("1147")},
	})
}
