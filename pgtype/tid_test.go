package pgtype_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestTIDCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type tid")

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "tid", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true},
			Result: new(pgtype.TID),
			Test:   isExpectedEq(pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true}),
		},
		{
			Param:  pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true},
			Result: new(pgtype.TID),
			Test:   isExpectedEq(pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true}),
		},
		{
			Param:  pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true},
			Result: new(string),
			Test:   isExpectedEq("(42,43)"),
		},
		{
			Param:  pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true},
			Result: new(string),
			Test:   isExpectedEq("(4294967295,65535)"),
		},
		{Param: pgtype.TID{}, Result: new(pgtype.TID), Test: isExpectedEq(pgtype.TID{})},
		{Param: nil, Result: new(pgtype.TID), Test: isExpectedEq(pgtype.TID{})},
	})
}
