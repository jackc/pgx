package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/testutil"
)

func TestTIDCodec(t *testing.T) {
	testutil.RunTranscodeTests(t, "tid", []testutil.TranscodeTestCase{
		{
			pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true},
			new(pgtype.TID),
			isExpectedEq(pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true}),
		},
		{
			pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true},
			new(pgtype.TID),
			isExpectedEq(pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true}),
		},
		{
			pgtype.TID{BlockNumber: 42, OffsetNumber: 43, Valid: true},
			new(string),
			isExpectedEq("(42,43)"),
		},
		{
			pgtype.TID{BlockNumber: 4294967295, OffsetNumber: 65535, Valid: true},
			new(string),
			isExpectedEq("(4294967295,65535)"),
		},
		{pgtype.TID{}, new(pgtype.TID), isExpectedEq(pgtype.TID{})},
		{nil, new(pgtype.TID), isExpectedEq(pgtype.TID{})},
	})
}
