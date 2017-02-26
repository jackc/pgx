package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/pgtype"
)

func TestTimestamptzTranscode(t *testing.T) {
	testSuccessfulTranscodeEqFunc(t, "timestamptz", []interface{}{
		pgtype.Timestamptz{Time: time.Date(1800, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(1905, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(1940, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(1960, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(1999, 12, 31, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(2000, 1, 2, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present},
		pgtype.Timestamptz{Status: pgtype.Null},
		pgtype.Timestamptz{Status: pgtype.Present, InfinityModifier: pgtype.Infinity},
		pgtype.Timestamptz{Status: pgtype.Present, InfinityModifier: -pgtype.Infinity},
	}, func(a, b interface{}) bool {
		at := a.(pgtype.Timestamptz)
		bt := b.(pgtype.Timestamptz)

		return at.Time.Equal(bt.Time) && at.Status == bt.Status && at.InfinityModifier == bt.InfinityModifier
	})
}

func TestTimestamptzConvertFrom(t *testing.T) {
	type _time time.Time

	successfulTests := []struct {
		source interface{}
		result pgtype.Timestamptz
	}{
		{source: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(1999, 12, 31, 12, 59, 59, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(1999, 12, 31, 12, 59, 59, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(2000, 1, 1, 0, 0, 1, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(2000, 1, 1, 0, 0, 1, 0, time.Local), Status: pgtype.Present}},
		{source: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), result: pgtype.Timestamptz{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
		{source: _time(time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local)), result: pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local), Status: pgtype.Present}},
	}

	for i, tt := range successfulTests {
		var d pgtype.Timestamptz
		err := d.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if d != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, d)
		}
	}
}
