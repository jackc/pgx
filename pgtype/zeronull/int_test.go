// Do not edit. Generated from pgtype/zeronull/int_test.go.erb
package zeronull_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
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

func TestInt4Transcode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int4", []interface{}{
		(zeronull.Int4)(1),
		(zeronull.Int4)(0),
	})
}

func TestInt4ConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "int4", (zeronull.Int4)(0))
}

func TestInt4ConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "int4", (zeronull.Int4)(0))
}

func TestInt8Transcode(t *testing.T) {
	testutil.TestSuccessfulTranscode(t, "int8", []interface{}{
		(zeronull.Int8)(1),
		(zeronull.Int8)(0),
	})
}

func TestInt8ConvertsGoZeroToNull(t *testing.T) {
	testutil.TestGoZeroToNullConversion(t, "int8", (zeronull.Int8)(0))
}

func TestInt8ConvertsNullToGoZero(t *testing.T) {
	testutil.TestNullToGoZeroConversion(t, "int8", (zeronull.Int8)(0))
}
