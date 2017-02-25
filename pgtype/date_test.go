package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/pgtype"
)

func TestDateTranscode(t *testing.T) {
	testSuccessfulTranscode(t, "date", []interface{}{
		pgtype.Date{Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
		pgtype.Date{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
		pgtype.Date{Time: time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
		pgtype.Date{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
		pgtype.Date{Time: time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
		pgtype.Date{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
		pgtype.Date{Status: pgtype.Null},
	})
}

func TestDateConvertFrom(t *testing.T) {
	type _time time.Time

	successfulTests := []struct {
		source interface{}
		result *pgtype.Date
	}{
		{source: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), result: &pgtype.Date{Time: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), result: &pgtype.Date{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), result: &pgtype.Date{Time: time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), result: &pgtype.Date{Time: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), result: &pgtype.Date{Time: time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)}},
		{source: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), result: &pgtype.Date{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{source: _time(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)), result: &pgtype.Date{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)}},
	}

	for i, tt := range successfulTests {
		var d pgtype.Date
		err := d.ConvertFrom(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
}
