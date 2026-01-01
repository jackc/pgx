package zeronull_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqTimestamptz(a any) func(any) bool {
	return func(v any) bool {
		at := time.Time(a.(zeronull.Timestamptz))
		vt := time.Time(v.(zeronull.Timestamptz))

		return at.Equal(vt)
	}
}

func TestTimestamptzTranscode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "timestamptz", []pgxtest.ValueRoundTripTest{
		{
			Param:  (zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)),
			Result: new(zeronull.Timestamptz),
			Test:   isExpectedEqTimestamptz((zeronull.Timestamptz)(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))),
		},
		{
			Param:  nil,
			Result: new(zeronull.Timestamptz),
			Test:   isExpectedEqTimestamptz((zeronull.Timestamptz)(time.Time{})),
		},
		{
			Param:  (zeronull.Timestamptz)(time.Time{}),
			Result: new(any),
			Test:   isExpectedEq(nil),
		},
	})
}
