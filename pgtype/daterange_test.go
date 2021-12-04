package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestDaterangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "daterange", []interface{}{
		&pgtype.Daterange{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
		&pgtype.Daterange{
			Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Daterange{
			Lower:     pgtype.Date{Time: time.Date(1800, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Date{Time: time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Daterange{},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Daterange)
		b := bb.(pgtype.Daterange)

		return a.Valid == b.Valid &&
			a.Lower.Time.Equal(b.Lower.Time) &&
			a.Lower.Valid == b.Lower.Valid &&
			a.Lower.InfinityModifier == b.Lower.InfinityModifier &&
			a.Upper.Time.Equal(b.Upper.Time) &&
			a.Upper.Valid == b.Upper.Valid &&
			a.Upper.InfinityModifier == b.Upper.InfinityModifier
	})
}

func TestDaterangeNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalizeEqFunc(t, []testutil.NormalizeTest{
		{
			SQL: "select daterange('2010-01-01', '2010-01-11', '(]')",
			Value: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(2010, 1, 2, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2010, 1, 12, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
		},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Daterange)
		b := bb.(pgtype.Daterange)

		return a.Valid == b.Valid &&
			a.Lower.Time.Equal(b.Lower.Time) &&
			a.Lower.Valid == b.Lower.Valid &&
			a.Lower.InfinityModifier == b.Lower.InfinityModifier &&
			a.Upper.Time.Equal(b.Upper.Time) &&
			a.Upper.Valid == b.Upper.Valid &&
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
			result: pgtype.Daterange{},
		},
		{
			source: &pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			result: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
		},
		{
			source: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			result: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
		},
		{
			source: "[1990-12-31,2028-01-01)",
			result: pgtype.Daterange{
				Lower:     pgtype.Date{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Date{Time: time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
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
