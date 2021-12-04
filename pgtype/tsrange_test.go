package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
)

func TestTsrangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "tsrange", []interface{}{
		&pgtype.Tsrange{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
		&pgtype.Tsrange{
			Lower:     pgtype.Timestamp{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Timestamp{Time: time.Date(2028, 1, 1, 0, 23, 12, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Tsrange{
			Lower:     pgtype.Timestamp{Time: time.Date(1800, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Timestamp{Time: time.Date(2200, 1, 1, 0, 23, 12, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Tsrange{},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Tsrange)
		b := bb.(pgtype.Tsrange)

		return a.Valid == b.Valid &&
			a.Lower.Time.Equal(b.Lower.Time) &&
			a.Lower.Valid == b.Lower.Valid &&
			a.Lower.InfinityModifier == b.Lower.InfinityModifier &&
			a.Upper.Time.Equal(b.Upper.Time) &&
			a.Upper.Valid == b.Upper.Valid &&
			a.Upper.InfinityModifier == b.Upper.InfinityModifier
	})
}
