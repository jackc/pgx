package pgtype_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
)

func TestIntervalCodec(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "interval", []pgxtest.ValueRoundTripTest{
		{
			Param:  pgtype.Interval{Microseconds: 1, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 1, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: 1_000_000, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 1_000_000, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: 1_000_001, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 1_000_001, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: 123_202_800_000_000, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 123_202_800_000_000, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Days: 1, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Days: 1, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Months: 1, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: 1, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Months: 12, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: 12, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Months: 13, Days: 15, Microseconds: 1_000_001, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: 13, Days: 15, Microseconds: 1_000_001, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: -1, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: -1, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: -1_000_000, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: -1_000_000, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: -1_000_001, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: -1_000_001, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Microseconds: -123_202_800_000_000, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: -123_202_800_000_000, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Days: -1, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Days: -1, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Months: -1, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: -1, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Months: -12, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: -12, Valid: true}),
		},
		{
			Param:  pgtype.Interval{Months: -13, Days: -15, Microseconds: -1_000_001, Valid: true},
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: -13, Days: -15, Microseconds: -1_000_001, Valid: true}),
		},
		{
			Param:  "1 second",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 1_000_000, Valid: true}),
		},
		{
			Param:  "1.000001 second",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 1_000_001, Valid: true}),
		},
		{
			Param:  "34223 hours",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Microseconds: 123_202_800_000_000, Valid: true}),
		},
		{
			Param:  "1 day",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Days: 1, Valid: true}),
		},
		{
			Param:  "1 month",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: 1, Valid: true}),
		},
		{
			Param:  "1 year",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: 12, Valid: true}),
		},
		{
			Param:  "-13 mon",
			Result: new(pgtype.Interval),
			Test:   isExpectedEq(pgtype.Interval{Months: -13, Valid: true}),
		},
		{Param: time.Hour, Result: new(time.Duration), Test: isExpectedEq(time.Hour)},
		{
			Param:  pgtype.Interval{Months: 1, Days: 1, Valid: true},
			Result: new(time.Duration),
			Test:   isExpectedEq(time.Duration(2_678_400_000_000_000)),
		},
		{Param: pgtype.Interval{}, Result: new(pgtype.Interval), Test: isExpectedEq(pgtype.Interval{})},
		{Param: nil, Result: new(pgtype.Interval), Test: isExpectedEq(pgtype.Interval{})},
	})
}

func TestIntervalTextEncode(t *testing.T) {
	m := pgtype.NewMap()

	successfulTests := []struct {
		source pgtype.Interval
		result string
	}{
		{source: pgtype.Interval{Months: 2, Days: 1, Microseconds: 0, Valid: true}, result: "2 mon 1 day 00:00:00"},
		{source: pgtype.Interval{Months: 0, Days: 0, Microseconds: 0, Valid: true}, result: "00:00:00"},
		{source: pgtype.Interval{Months: 0, Days: 0, Microseconds: 6 * 60 * 1_000_000, Valid: true}, result: "00:06:00"},
		{source: pgtype.Interval{Months: 0, Days: 1, Microseconds: 6*60*1_000_000 + 30, Valid: true}, result: "1 day 00:06:00.000030"},
	}
	for i, tt := range successfulTests {
		buf, err := m.Encode(pgtype.DateOID, pgtype.TextFormatCode, tt.source, nil)
		assert.NoErrorf(t, err, "%d", i)
		assert.Equalf(t, tt.result, string(buf), "%d", i)
	}
}
