package zeronull_test

import (
	"testing"

	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgtype/zeronull"
)

func TestTextTranscode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "text", []interface{}{
		(zeronull.Text)("foo"),
		(zeronull.Text)(""),
	})
}

func TestTextConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "text", (zeronull.Text)(""))
}

func TestTextConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "text", (zeronull.Text)(""))
}
