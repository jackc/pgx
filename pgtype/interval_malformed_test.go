package pgtype_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// A malformed interval whose time component has an empty hours segment
// (for example ":05:06") must return an error instead of panicking with an
// index out of range on timeParts[0][0].
func TestIntervalScanMalformedTimeReturnsError(t *testing.T) {
	for _, src := range []string{":05:06", "1 day :30:00"} {
		var v pgtype.Interval
		if err := v.Scan(src); err == nil {
			t.Errorf("Scan(%q): expected error, got nil", src)
		}
	}
}
