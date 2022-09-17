package zeronull_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqTimestamp(a any) func(any) bool {
	return func(v any) bool {
		at := time.Time(a.(zeronull.Timestamp))
		vt := time.Time(v.(zeronull.Timestamp))

		return at.Equal(vt)
	}
}

func TestTimestampTranscode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "timestamp", []pgxtest.ValueRoundTripTest{
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
			new(any),
			isExpectedEq(nil),
		},
	})
}
