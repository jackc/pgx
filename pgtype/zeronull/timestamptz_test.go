package zeronull_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
)

func isExpectedEqTimestamptz(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		at := time.Time(a.(zeronull.Timestamptz))
		vt := time.Time(v.(zeronull.Timestamptz))

		return at.Equal(vt)
	}
}

func TestTimestamptzTranscode(t *testing.T) {
	testutil.RunTranscodeTests(t, "timestamptz", []testutil.TranscodeTestCase{
		{
			(zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
			new(zeronull.Timestamptz),
			isExpectedEqTimestamptz((zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))),
		},
		{
			nil,
			new(zeronull.Timestamptz),
			isExpectedEqTimestamptz((zeronull.Timestamptz)(time.Time{})),
		},
		{
			(zeronull.Timestamptz)(time.Time{}),
			new(interface{}),
			isExpectedEq(nil),
		},
	})
}
