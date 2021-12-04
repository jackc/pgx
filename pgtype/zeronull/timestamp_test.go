package zeronull_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgtype/zeronull"
)

func TestTimestampTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "timestamp", []interface{}{
		(zeronull.Timestamp)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		(zeronull.Timestamp)(time.Time{}),
	}, func(a, b interface{}) bool {
		at := a.(zeronull.Timestamp)
		bt := b.(zeronull.Timestamp)

		return time.Time(at).Equal(time.Time(bt))
	})
}

func TestTimestampConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "timestamp", (zeronull.Timestamp)(time.Time{}))
}

func TestTimestampConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "timestamp", (zeronull.Timestamp)(time.Time{}))
}
