package zeronull_test

import (
	"testing"

	"github.com/jackc/pgtype/testutil"
	"github.com/jackc/pgtype/zeronull"
)

func TestInt2Transcode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int2", []interface{}{
		(zeronull.Int2)(1),
		(zeronull.Int2)(0),
	})
}

func TestInt2ConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "int2", (zeronull.Int2)(0))
}

func TestInt2ConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "int2", (zeronull.Int2)(0))
}
