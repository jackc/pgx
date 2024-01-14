package pgx

import (
	"testing"
)

// SetMaxLargeObjectMessageLength sets internal maxLargeObjectMessageLength variable
// to the given length for the duration of the test.
//
// Tests using this helper should not use t.Parallel().
func SetMaxLargeObjectMessageLength(t *testing.T, length int) {
	t.Helper()

	original := maxLargeObjectMessageLength
	t.Cleanup(func() {
		maxLargeObjectMessageLength = original
	})

	maxLargeObjectMessageLength = length
}
