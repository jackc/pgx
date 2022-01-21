package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestTIDCodec(t *testing.T) {
	testPgxCodec(t, "tid", []PgxTranscodeTestCase{
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
		{pgtype.TID{}, new(pgtype.TID), isExpectedEq(pgtype.TID{})},
		{nil, new(pgtype.TID), isExpectedEq(pgtype.TID{})},
	})
}
