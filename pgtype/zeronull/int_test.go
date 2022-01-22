// Do not edit. Generated from pgtype/zeronull/int_test.go.erb
package zeronull_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype/testutil"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
)

func TestInt2Transcode(t *testing.T) {
	testutil.RunTranscodeTests(t, "int2", []testutil.TranscodeTestCase{
		{
			(zeronull.Int2)(1),
			new(zeronull.Int2),
			isExpectedEq((zeronull.Int2)(1)),
		},
		{
			nil,
			new(zeronull.Int2),
			isExpectedEq((zeronull.Int2)(0)),
		},
		{
			(zeronull.Int2)(0),
			new(interface{}),
			isExpectedEq(nil),
		},
	})
}

func TestInt4Transcode(t *testing.T) {
	testutil.RunTranscodeTests(t, "int4", []testutil.TranscodeTestCase{
		{
			(zeronull.Int4)(1),
			new(zeronull.Int4),
			isExpectedEq((zeronull.Int4)(1)),
		},
		{
			nil,
			new(zeronull.Int4),
			isExpectedEq((zeronull.Int4)(0)),
		},
		{
			(zeronull.Int4)(0),
			new(interface{}),
			isExpectedEq(nil),
		},
	})
}

func TestInt8Transcode(t *testing.T) {
	testutil.RunTranscodeTests(t, "int8", []testutil.TranscodeTestCase{
		{
			(zeronull.Int8)(1),
			new(zeronull.Int8),
			isExpectedEq((zeronull.Int8)(1)),
		},
		{
			nil,
			new(zeronull.Int8),
			isExpectedEq((zeronull.Int8)(0)),
		},
		{
			(zeronull.Int8)(0),
			new(interface{}),
			isExpectedEq(nil),
		},
	})
}
