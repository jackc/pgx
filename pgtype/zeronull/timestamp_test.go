package zeronull_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
)

func isExpectedEqTimestamp(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		at := time.Time(a.(zeronull.Timestamp))
		vt := time.Time(v.(zeronull.Timestamp))

		return at.Equal(vt)
	}
}

func TestTimestampTranscode(t *testing.T) {
	testutil.RunTranscodeTests(t, "timestamp", []testutil.TranscodeTestCase{
		{
			(zeronull.Timestamp)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
			new(zeronull.Timestamp),
			isExpectedEqTimestamp((zeronull.Timestamp)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))),
		},
		{
			nil,
			new(zeronull.Timestamp),
			isExpectedEqTimestamp((zeronull.Timestamp)(time.Time{})),
		},
		{
			(zeronull.Timestamp)(time.Time{}),
			new(interface{}),
			isExpectedEq(nil),
		},
	})
}
