package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestDaterangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "daterange", []interface{}{
		&pgtype.Daterange{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Status: pgtype.Present},
		&pgtype.Daterange{
			Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
			Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Status:    pgtype.Present,
		},
		&pgtype.Daterange{
			Lower:     pgtype.Date{Time: time.Date(1800, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
			Upper:     pgtype.Date{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Status:    pgtype.Present,
		},
		&pgtype.Daterange{Status: pgtype.Null},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Daterange)
		b := bb.(pgtype.Daterange)

		return a.Status == b.Status &&
			a.Lower.Time.Equal(b.Lower.Time) &&
			a.Lower.Status == b.Lower.Status &&
			a.Lower.InfinityModifier == b.Lower.InfinityModifier &&
			a.Upper.Time.Equal(b.Upper.Time) &&
			a.Upper.Status == b.Upper.Status &&
			a.Upper.InfinityModifier == b.Upper.InfinityModifier
	})
}

func TestDaterangeNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalizeEqFunc(t, []testutil.NormalizeTest{
		{
			SQL: "select daterange('2010-01-01', '2010-01-11', '(]')",
			Value: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(2010, 1, 2, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				Upper:     pgtype.Date{Time: time.Date(2010, 1, 12, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Status:    pgtype.Present,
			},
		},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Daterange)
		b := bb.(pgtype.Daterange)

		return a.Status == b.Status &&
			a.Lower.Time.Equal(b.Lower.Time) &&
			a.Lower.Status == b.Lower.Status &&
			a.Lower.InfinityModifier == b.Lower.InfinityModifier &&
			a.Upper.Time.Equal(b.Upper.Time) &&
			a.Upper.Status == b.Upper.Status &&
			a.Upper.InfinityModifier == b.Upper.InfinityModifier
	})
}

func TestDaterangeSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result pgtype.Daterange
	}{
		{
			source: nil,
			result: pgtype.Daterange{Status: pgtype.Null},
		},
		{
			source: &pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Status:    pgtype.Present,
			},
			result: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Status:    pgtype.Present,
			},
		},
		{
			source: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Status:    pgtype.Present,
			},
			result: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Status:    pgtype.Present,
			},
		},
		{
			source: "[1990-12-31,2028-01-01)",
			result: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Status: pgtype.Present},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Status:    pgtype.Present,
			},
		},
	}

	for i, tt := range successfulTests {
		var r pgtype.Daterange
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if r != tt.result {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}
