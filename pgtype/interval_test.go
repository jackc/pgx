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
			pgtype.Interval{Microseconds: 1, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 1, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: 1000000, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 1000000, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: 1000001, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 1000001, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: 123202800000000, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 123202800000000, Valid: true}),
		},
		{
			pgtype.Interval{Days: 1, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Days: 1, Valid: true}),
		},
		{
			pgtype.Interval{Months: 1, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: 1, Valid: true}),
		},
		{
			pgtype.Interval{Months: 12, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: 12, Valid: true}),
		},
		{
			pgtype.Interval{Months: 13, Days: 15, Microseconds: 1000001, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: 13, Days: 15, Microseconds: 1000001, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: -1, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: -1, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: -1000000, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: -1000000, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: -1000001, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: -1000001, Valid: true}),
		},
		{
			pgtype.Interval{Microseconds: -123202800000000, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: -123202800000000, Valid: true}),
		},
		{
			pgtype.Interval{Days: -1, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Days: -1, Valid: true}),
		},
		{
			pgtype.Interval{Months: -1, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: -1, Valid: true}),
		},
		{
			pgtype.Interval{Months: -12, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: -12, Valid: true}),
		},
		{
			pgtype.Interval{Months: -13, Days: -15, Microseconds: -1000001, Valid: true},
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: -13, Days: -15, Microseconds: -1000001, Valid: true}),
		},
		{
			"1 second",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 1000000, Valid: true}),
		},
		{
			"1.000001 second",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 1000001, Valid: true}),
		},
		{
			"34223 hours",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Microseconds: 123202800000000, Valid: true}),
		},
		{
			"1 day",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Days: 1, Valid: true}),
		},
		{
			"1 month",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: 1, Valid: true}),
		},
		{
			"1 year",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: 12, Valid: true}),
		},
		{
			"-13 mon",
			new(pgtype.Interval),
			isExpectedEq(pgtype.Interval{Months: -13, Valid: true}),
		},
		{time.Hour, new(time.Duration), isExpectedEq(time.Hour)},
		{
			pgtype.Interval{Months: 1, Days: 1, Valid: true},
			new(time.Duration),
			isExpectedEq(time.Duration(2678400000000000)),
		},
		{pgtype.Interval{}, new(pgtype.Interval), isExpectedEq(pgtype.Interval{})},
		{nil, new(pgtype.Interval), isExpectedEq(pgtype.Interval{})},
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
		{source: pgtype.Interval{Months: 0, Days: 0, Microseconds: 6 * 60 * 1000000, Valid: true}, result: "00:06:00"},
		{source: pgtype.Interval{Months: 0, Days: 1, Microseconds: 6*60*1000000 + 30, Valid: true}, result: "1 day 00:06:00.000030"},
	}
	for i, tt := range successfulTests {
		buf, err := m.Encode(pgtype.DateOID, pgtype.TextFormatCode, tt.source, nil)
		assert.NoErrorf(t, err, "%d", i)
		assert.Equalf(t, tt.result, string(buf), "%d", i)
	}
}
