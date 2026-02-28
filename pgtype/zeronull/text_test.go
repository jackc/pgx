package zeronull_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxtest"
)

func TestTextTranscode(t *testing.T) {
	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "text", []pgxtest.ValueRoundTripTest{
		{
			Param:  (zeronull.Text)("foo"),
			Result: new(zeronull.Text),
			Test:   isExpectedEq((zeronull.Text)("foo")),
		},
		{
			Param:  nil,
			Result: new(zeronull.Text),
			Test:   isExpectedEq((zeronull.Text)("")),
		},
		{
			Param:  (zeronull.Text)(""),
			Result: new(any),
			Test:   isExpectedEq(nil),
		},
	})
}
