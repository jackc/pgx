package pgtype_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestTimeCodec(t *testing.T) {
	testutil.RunTranscodeTests(t, "time", []testutil.TranscodeTestCase{
		{
			pgtype.Time{Microseconds: 0, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 0, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 1, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 1, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 86399999999, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 86399999999, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 86400000000, Valid: true},
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 86400000000, Valid: true}),
		},
		{
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			new(pgtype.Time),
			isExpectedEq(pgtype.Time{Microseconds: 0, Valid: true}),
		},
		{
			pgtype.Time{Microseconds: 0, Valid: true},
			new(time.Time),
			isExpectedEq(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{pgtype.Time{}, new(pgtype.Time), isExpectedEq(pgtype.Time{})},
		{nil, new(pgtype.Time), isExpectedEq(pgtype.Time{})},
	})
}
