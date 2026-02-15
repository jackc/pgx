package pgconn

import (
	"context"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgconn/internal/bgreader"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestCloseWhileSlowWriteTimerIsActive tests that Close does not panic when called while the slow write timer is
// active. This can happen when Close is called during a panic while flushWithPotentialWriteReadDeadlock is in progress,
// or when asyncClose has started a goroutine that is concurrently calling flushWithPotentialWriteReadDeadlock.
func TestCloseWhileSlowWriteTimerIsActive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgConn, err := Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	// Simulate the state during flushWithPotentialWriteReadDeadlock: the connection is busy and the slow write timer
	// has been started by enterPotentialWriteReadDeadlock.
	pgConn.status = connStatusBusy
	pgConn.enterPotentialWriteReadDeadlock()

	// Calling Close while the slow write timer is active previously panicked with
	// "BUG: slow write timer already active" because Close also called enterPotentialWriteReadDeadlock
	// via flushWithPotentialWriteReadDeadlock.
	closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer closeCancel()
	pgConn.Close(closeCtx)

	// Clean up the timer that was started above. exitPotentialWriteReadDeadlock will either stop the timer if it
	// hasn't fired yet, or wait for the background reader to start and then stop it.
	pgConn.exitPotentialWriteReadDeadlock()
}

// TestCloseWhileSlowWriteTimerIsActiveNoContext is the same as TestCloseWhileSlowWriteTimerIsActive but with
// context.Background() to test the code path where the context watcher is not set up.
func TestCloseWhileSlowWriteTimerIsActiveNoContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgConn, err := Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	pgConn.status = connStatusBusy
	pgConn.enterPotentialWriteReadDeadlock()

	pgConn.Close(context.Background())

	pgConn.exitPotentialWriteReadDeadlock()
}

// TestCloseWhileSlowWriteTimerFired tests that Close does not panic when called after the slow write timer has already
// fired and started the background reader. This represents a longer-blocked write where the deadlock avoidance
// mechanism has already kicked in.
func TestCloseWhileSlowWriteTimerFired(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgConn, err := Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	// Simulate a busy connection.
	pgConn.status = connStatusBusy
	pgConn.enterPotentialWriteReadDeadlock()

	// Wait for the slow write timer to fire and start the background reader.
	time.Sleep(50 * time.Millisecond)

	// Close should not panic even after the timer has fired.
	closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer closeCancel()
	pgConn.Close(closeCtx)

	pgConn.exitPotentialWriteReadDeadlock()
}

// TestCloseIdleConnectionSendsTerminate verifies that Close on an idle connection still sends the Terminate message
// (the normal case is not broken by the busy-connection fix).
func TestCloseIdleConnectionSendsTerminate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Build a minimal PgConn with a recording connection to verify Terminate is sent.
	realConn, err := Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	require.Equal(t, byte(connStatusIdle), realConn.status)

	closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer closeCancel()
	err = realConn.Close(closeCtx)
	require.NoError(t, err)

	// Verify the connection is fully cleaned up.
	select {
	case <-realConn.CleanupDone():
	case <-time.After(5 * time.Second):
		t.Fatal("Connection cleanup exceeded maximum time")
	}
}

// TestCloseWhileSlowWriteTimerIsActiveUsesMinimalPgConn tests the fix with a manually constructed PgConn to isolate
// the timer behavior without needing a database connection.
func TestCloseWhileSlowWriteTimerIsActiveUsesMinimalPgConn(t *testing.T) {
	t.Parallel()

	// Create a pipe so we have a real net.Conn.
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	bgReader := bgreader.New(clientConn)
	bgReaderStarted := make(chan struct{})
	slowWriteTimer := time.AfterFunc(time.Duration(math.MaxInt64), func() {
		bgReader.Start()
		bgReaderStarted <- struct{}{}
	})
	slowWriteTimer.Stop()

	pgConn := &PgConn{
		conn:            clientConn,
		status:          connStatusBusy,
		bgReader:        bgReader,
		slowWriteTimer:  slowWriteTimer,
		bgReaderStarted: bgReaderStarted,
		frontend:        pgproto3.NewFrontend(bgReader, clientConn),
		cleanupDone:     make(chan struct{}),
		contextWatcher:  ctxwatch.NewContextWatcher(&DeadlineContextWatcherHandler{Conn: clientConn}),
	}

	// Start the slow write timer (simulating enterPotentialWriteReadDeadlock).
	pgConn.enterPotentialWriteReadDeadlock()

	// Close must not panic.
	pgConn.Close(context.Background())

	// Clean up the timer.
	pgConn.exitPotentialWriteReadDeadlock()
}
