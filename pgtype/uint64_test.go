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

func TestUint64CodecSQLScanner(t *testing.T) {
	m := pgtype.NewMap()
	for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
		for i, tt := range []struct {
			value any
			want  any
		}{
			{uint64(1<<63 - 1), int64(1<<63 - 1)},
			{uint64(1 << 63), "9223372036854775808"},
			{uint64(1<<64 - 1), "18446744073709551615"},
			{nil, nil},
		} {
			buf, err := m.Encode(pgtype.XID8OID, format, tt.value, nil)
			if err != nil {
				t.Fatal(err)
			}

			called := false
			err = m.Scan(pgtype.XID8OID, format, buf, sqlScannerFunc(func(src any) error {
				called = true
				if src != tt.want {
					t.Errorf("%d. format %d: got %v (%T), want %v (%T)", i, format, src, src, tt.want, tt.want)
				}
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("sql.Scanner was not called")
			}
		}
	}
}
