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
			Param:  (zeronull.Timestamp)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
			Result: new(zeronull.Timestamp),
			Test:   isExpectedEqTimestamp((zeronull.Timestamp)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))),
		},
		{
			Param:  nil,
			Result: new(zeronull.Timestamp),
			Test:   isExpectedEqTimestamp((zeronull.Timestamp)(time.Time{})),
		},
		{
			Param:  (zeronull.Timestamp)(time.Time{}),
			Result: new(any),
			Test:   isExpectedEq(nil),
		},
	})
}
