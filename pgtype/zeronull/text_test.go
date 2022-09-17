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
			(zeronull.Text)("foo"),
			new(zeronull.Text),
			isExpectedEq((zeronull.Text)("foo")),
		},
		{
			nil,
			new(zeronull.Text),
			isExpectedEq((zeronull.Text)("")),
		},
		{
			(zeronull.Text)(""),
			new(any),
			isExpectedEq(nil),
		},
	})
}
