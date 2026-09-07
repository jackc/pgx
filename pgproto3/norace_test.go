//go:build !race

package pgproto3_test

import "testing"

// skipIfRaceEnabled does nothing when the race detector is disabled. See race_test.go for why the race detector build
// skips these tests.
func skipIfRaceEnabled(t *testing.T) {
	t.Helper()
}
