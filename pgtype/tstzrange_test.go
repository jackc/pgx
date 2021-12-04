package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

func TestTstzrangeTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "tstzrange", []interface{}{
		&pgtype.Tstzrange{LowerType: pgtype.Empty, UpperType: pgtype.Empty, Valid: true},
		&pgtype.Tstzrange{
			Lower:     pgtype.Timestamptz{Time: time.Date(1990, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Timestamptz{Time: time.Date(2028, 1, 1, 0, 23, 12, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Tstzrange{
			Lower:     pgtype.Timestamptz{Time: time.Date(1800, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     pgtype.Timestamptz{Time: time.Date(2200, 1, 1, 0, 23, 12, 0, time.UTC), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		&pgtype.Tstzrange{},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Tstzrange)
		b := bb.(pgtype.Tstzrange)

		return a.Valid == b.Valid &&
			a.Lower.Time.Equal(b.Lower.Time) &&
			a.Lower.Valid == b.Lower.Valid &&
			a.Lower.InfinityModifier == b.Lower.InfinityModifier &&
			a.Upper.Time.Equal(b.Upper.Time) &&
			a.Upper.Valid == b.Upper.Valid &&
			a.Upper.InfinityModifier == b.Upper.InfinityModifier
	})
}

// https://github.com/jackc/pgtype/issues/74
func TestTstzRangeDecodeTextInvalid(t *testing.T) {
	tstzrange := &pgtype.Tstzrange{}
	err := tstzrange.DecodeText(nil, []byte(`[eeee,)`))
	require.Error(t, err)
}
