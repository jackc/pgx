package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestUint32Codec(t *testing.T) {
	testPgxCodec(t, "oid", []PgxTranscodeTestCase{
		{
			pgtype.Uint32{Uint: pgtype.TextOID, Valid: true},
			new(pgtype.Uint32),
			isExpectedEq(pgtype.Uint32{Uint: pgtype.TextOID, Valid: true}),
		},
		{pgtype.Uint32{}, new(pgtype.Uint32), isExpectedEq(pgtype.Uint32{})},
		{nil, new(pgtype.Uint32), isExpectedEq(pgtype.Uint32{})},
	})
}
