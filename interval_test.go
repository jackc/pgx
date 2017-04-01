package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func TestIntervalTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "interval", []interface{}{
		pgtype.Interval{Microseconds: 1, Status: pgtype.Present},
		pgtype.Interval{Microseconds: 1000000, Status: pgtype.Present},
		pgtype.Interval{Microseconds: 1000001, Status: pgtype.Present},
		pgtype.Interval{Microseconds: 123202800000000, Status: pgtype.Present},
		pgtype.Interval{Days: 1, Status: pgtype.Present},
		pgtype.Interval{Months: 1, Status: pgtype.Present},
		pgtype.Interval{Months: 12, Status: pgtype.Present},
		pgtype.Interval{Months: 13, Days: 15, Microseconds: 1000001, Status: pgtype.Present},
		pgtype.Interval{Microseconds: -1, Status: pgtype.Present},
		pgtype.Interval{Microseconds: -1000000, Status: pgtype.Present},
		pgtype.Interval{Microseconds: -1000001, Status: pgtype.Present},
		pgtype.Interval{Microseconds: -123202800000000, Status: pgtype.Present},
		pgtype.Interval{Days: -1, Status: pgtype.Present},
		pgtype.Interval{Months: -1, Status: pgtype.Present},
		pgtype.Interval{Months: -12, Status: pgtype.Present},
		pgtype.Interval{Months: -13, Days: -15, Microseconds: -1000001, Status: pgtype.Present},
		pgtype.Interval{Status: pgtype.Null},
	})
}

func TestIntervalNormalize(t *testing.T) {
	testSuccessfulNormalize(t, []normalizeTest{
		{
			sql:   "select '1 second'::interval",
			value: pgtype.Interval{Microseconds: 1000000, Status: pgtype.Present},
		},
		{
			sql:   "select '1.000001 second'::interval",
			value: pgtype.Interval{Microseconds: 1000001, Status: pgtype.Present},
		},
		{
			sql:   "select '34223 hours'::interval",
			value: pgtype.Interval{Microseconds: 123202800000000, Status: pgtype.Present},
		},
		{
			sql:   "select '1 day'::interval",
			value: pgtype.Interval{Days: 1, Status: pgtype.Present},
		},
		{
			sql:   "select '1 month'::interval",
			value: pgtype.Interval{Months: 1, Status: pgtype.Present},
		},
		{
			sql:   "select '1 year'::interval",
			value: pgtype.Interval{Months: 12, Status: pgtype.Present},
		},
		{
			sql:   "select '-13 mon'::interval",
			value: pgtype.Interval{Months: -13, Status: pgtype.Present},
		},
	})
}
