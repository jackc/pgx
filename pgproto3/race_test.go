//go:build race

package pgproto3_test

import "testing"

// skipIfRaceEnabled skips tests that allocate a message of the maximum allowed size (~1GB). The race detector's shadow
// memory multiplies the memory used by these tests by roughly 8x, which is enough to exhaust the memory of a CI runner
// and get the entire job killed. These tests are single goroutine encoding tests, so there is nothing for the race
// detector to find in them anyway. CI runs the Windows tests without the race detector, so they are still covered.
func skipIfRaceEnabled(t *testing.T) {
	t.Helper()
	t.Skip("skipping test that allocates ~1GB because the race detector is enabled")
}
