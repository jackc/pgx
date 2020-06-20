package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "interval", []interface{}{
		&pgtype.Interval{Microseconds: 1, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: 1000000, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: 1000001, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: 123202800000000, Status: pgtype.Present},
		&pgtype.Interval{Days: 1, Status: pgtype.Present},
		&pgtype.Interval{Months: 1, Status: pgtype.Present},
		&pgtype.Interval{Months: 12, Status: pgtype.Present},
		&pgtype.Interval{Months: 13, Days: 15, Microseconds: 1000001, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: -1, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: -1000000, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: -1000001, Status: pgtype.Present},
		&pgtype.Interval{Microseconds: -123202800000000, Status: pgtype.Present},
		&pgtype.Interval{Days: -1, Status: pgtype.Present},
		&pgtype.Interval{Months: -1, Status: pgtype.Present},
		&pgtype.Interval{Months: -12, Status: pgtype.Present},
		&pgtype.Interval{Months: -13, Days: -15, Microseconds: -1000001, Status: pgtype.Present},
		&pgtype.Interval{Status: pgtype.Null},
	})
}

func TestIntervalNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL:   "select '1 second'::interval",
			Value: &pgtype.Interval{Microseconds: 1000000, Status: pgtype.Present},
		},
		{
			SQL:   "select '1.000001 second'::interval",
			Value: &pgtype.Interval{Microseconds: 1000001, Status: pgtype.Present},
		},
		{
			SQL:   "select '34223 hours'::interval",
			Value: &pgtype.Interval{Microseconds: 123202800000000, Status: pgtype.Present},
		},
		{
			SQL:   "select '1 day'::interval",
			Value: &pgtype.Interval{Days: 1, Status: pgtype.Present},
		},
		{
			SQL:   "select '1 month'::interval",
			Value: &pgtype.Interval{Months: 1, Status: pgtype.Present},
		},
		{
			SQL:   "select '1 year'::interval",
			Value: &pgtype.Interval{Months: 12, Status: pgtype.Present},
		},
		{
			SQL:   "select '-13 mon'::interval",
			Value: &pgtype.Interval{Months: -13, Status: pgtype.Present},
		},
	})
}

func TestIntervalLossyConversionToDuration(t *testing.T) {
	interval := &pgtype.Interval{Months: 1, Days: 1, Status: pgtype.Present}
	var d time.Duration
	err := interval.AssignTo(&d)
	require.NoError(t, err)
	assert.EqualValues(t, int64(2678400000000000), d.Nanoseconds())
}
