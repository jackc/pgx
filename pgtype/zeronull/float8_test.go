package zeronull_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
)

func isExpectedEq(a interface{}) func(interface{}) bool {
	return func(v interface{}) bool {
		return a == v
	}
}

func TestFloat8Transcode(t *testing.T) {
	testutil.RunTranscodeTests(t, "float8", []testutil.TranscodeTestCase{
		{
			(zeronull.Float8)(1),
			new(zeronull.Float8),
			isExpectedEq((zeronull.Float8)(1)),
		},
		{
			nil,
			new(zeronull.Float8),
			isExpectedEq((zeronull.Float8)(0)),
		},
		{
			(zeronull.Float8)(0),
			new(interface{}),
			isExpectedEq(nil),
		},
	})
}
