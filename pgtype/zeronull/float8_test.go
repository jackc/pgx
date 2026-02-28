package zeronull_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEq(a any) func(any) bool {
	return func(v any) bool {
		return a == v
	}
}

func TestFloat8Transcode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "float8", []pgxtest.ValueRoundTripTest{
		{
			Param:  (zeronull.Float8)(1),
			Result: new(zeronull.Float8),
			Test:   isExpectedEq((zeronull.Float8)(1)),
		},
		{
			Param:  nil,
			Result: new(zeronull.Float8),
			Test:   isExpectedEq((zeronull.Float8)(0)),
		},
		{
			Param:  (zeronull.Float8)(0),
			Result: new(any),
			Test:   isExpectedEq(nil),
		},
	})
}
