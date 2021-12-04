package zeronull_test

import (
	"testing"
	"time"

	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgtype/zeronull"
)

func TestTimestamptzTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscodeEqFunc(t, "timestamptz", []interface{}{
		(zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
		(zeronull.Timestamptz)(time.Time{}),
	}, func(a, b interface{}) bool {
		at := a.(zeronull.Timestamptz)
		bt := b.(zeronull.Timestamptz)

		return time.Time(at).Equal(time.Time(bt))
	})
}

func TestTimestamptzConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "timestamptz", (zeronull.Timestamptz)(time.Time{}))
}

func TestTimestamptzConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "timestamptz", (zeronull.Timestamptz)(time.Time{}))
}
