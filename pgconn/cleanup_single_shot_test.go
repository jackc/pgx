package pgconn

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// https://github.com/jackc/pgx/issues/2622
//
// The status guard around cleanupDone used to be a non-atomic test-and-set,
// so two close paths (Close, asyncClose, receiveMessage's OnPgError branch,
// CopyFrom's buffering-receive error branch) could both pass it and both call
// close(cleanupDone), panicking with "close of closed channel".
//
// These test the mechanism the fix relies on directly: exactly one goroutine
// can win markClosed, and finishCleanup is safe to call from any number of
// goroutines at once.
func TestMarkClosedIsSingleShot(t *testing.T) {
	t.Parallel()

	pgConn := &PgConn{cleanupDone: make(chan struct{})}

	const callers = 128
	results := make(chan bool, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- pgConn.markClosed()
		}()
	}
	wg.Wait()
	close(results)

	won := 0
	for r := range results {
		if r {
			won++
		}
	}

	require.Equal(t, 1, won, "exactly one goroutine must be reported as the closer")
	require.False(t, pgConn.markClosed(), "markClosed must report false once already closed")
	require.True(t, pgConn.IsClosed(), "connection must be reported closed after markClosed")
}

func TestFinishCleanupIsSingleShot(t *testing.T) {
	t.Parallel()

	pgConn := &PgConn{cleanupDone: make(chan struct{})}

	const callers = 128
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Must not panic by closing the channel more than once.
			pgConn.finishCleanup()
		}()
	}
	wg.Wait()

	select {
	case <-pgConn.cleanupDone:
	default:
		t.Fatal("cleanupDone must be closed after finishCleanup")
	}
}
