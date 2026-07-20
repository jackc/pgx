package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// A binary multirange value shorter than the 4-byte element-count header must
// return an error instead of panicking with "index out of range". decodeBinary
// already guards the per-element length prefixes and the element count against
// the remaining bytes, but it read the leading count without a length check, so
// a truncated server message of 1-3 bytes panicked.
func TestMultirangeBinaryDecodeTruncatedReturnsError(t *testing.T) {
	m := pgtype.NewMap()

	for _, src := range [][]byte{
		{},
		{0x00},
		{0x00, 0x00},
		{0x00, 0x00, 0x00},
	} {
		var v pgtype.Multirange[pgtype.Range[pgtype.Int4]]
		plan := m.PlanScan(pgtype.Int4multirangeOID, pgtype.BinaryFormatCode, &v)
		// A panic here (pre-fix) fails the test.
		if err := plan.Scan(src, &v); err == nil {
			t.Errorf("Scan(% x) = nil error, want an error", src)
		}
	}
}

// A well-formed empty multirange (element count 0) still decodes without error.
func TestMultirangeBinaryDecodeEmpty(t *testing.T) {
	m := pgtype.NewMap()

	var v pgtype.Multirange[pgtype.Range[pgtype.Int4]]
	plan := m.PlanScan(pgtype.Int4multirangeOID, pgtype.BinaryFormatCode, &v)
	if err := plan.Scan([]byte{0x00, 0x00, 0x00, 0x00}, &v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v) != 0 {
		t.Errorf("got %+v, want an empty multirange", v)
	}
}
