package pgconn_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

const pgSleepBlock = "pg_sleep(10)"

func buildCancelAndDrainConfig(t *testing.T) *pgconn.Config {
	t.Helper()
	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelAndDrainContextWatcherHandler{
			Conn:          conn,
			DeadlineDelay: 5 * time.Second,
			DrainTimeout:  5 * time.Second,
		}
	}
	config.ConnectTimeout = 5 * time.Second
	return config
}

// waitUntilActive polls pg_stat_activity from observer until targetPID is in "active" state, then
// returns. If the poll fails unexpectedly (e.g. PID vanishes), t.Errorf surfaces the diagnostic.
func waitUntilActive(t *testing.T, ctx context.Context, observer *pgconn.PgConn, targetPID []byte) {
	t.Helper()
	var polls int
	for {
		result := observer.ExecParams(ctx,
			"SELECT state FROM pg_stat_activity WHERE pid = $1",
			[][]byte{targetPID}, nil, nil, nil,
		).Read()
		polls++
		if result.Err != nil {
			if ctx.Err() == nil {
				t.Errorf("waitUntilActive: poll failed for pid %s after %d polls: %v", targetPID, polls, result.Err)
			}
			return
		}
		if len(result.Rows) == 0 {
			t.Errorf("waitUntilActive: pid %s not found in pg_stat_activity after %d polls", targetPID, polls)
			return
		}
		if string(result.Rows[0][0]) == "active" {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// cancelOnActive creates a cancellable child of t.Context(), starts polling pg_stat_activity in a
// goroutine, and returns the child context and a cleanup function.
//
// The cleanup function cancels the context (stopping the poller) AND waits for the goroutine to
// finish its last ExecParams on the observer. Callers MUST call cleanup before reusing the observer.
//
// <-ctx.Done() is NOT a safe synchronization point here because the caller may also call cancel
// (via cleanup) to break out of a deadlock when Exec returns before the poller fires. In that case,
// ctx.Done() closes immediately -- before the goroutine finishes with the observer. The WaitGroup
// inside cleanup is what actually signals goroutine completion.
func cancelOnActive(t *testing.T, observer *pgconn.PgConn, targetPID []byte) (ctx context.Context, cleanup func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		waitUntilActive(t, ctx, observer, targetPID)
	}()
	return ctx, func() {
		cancel()
		wg.Wait()
	}
}

func getBackendPID(t *testing.T, conn *pgconn.PgConn) []byte {
	t.Helper()
	result := conn.ExecParams(t.Context(),
		"SELECT pg_backend_pid()::TEXT", nil, nil, nil, nil,
	).Read()
	require.NoError(t, result.Err)
	require.Equal(t, 1, len(result.Rows))
	return result.Rows[0][0]
}

func newObserver(t *testing.T) *pgconn.PgConn {
	t.Helper()
	config, err := pgconn.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnectTimeout = 5 * time.Second
	conn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// Scenario 1: Cancel arrives before query completes
//
// The 57014 is consumed by the original query. Connection is clean afterward. The drain sends ";"
// anyway (harmless -- one extra round-trip).
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(sql)─────────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │  [ctx cancelled]      │                     │                  │
//	    │                       │                     │──CancelReq──────▶│
//	    │                       │                     │                  │──SIGINT──▶│
//	    │                       │ [interrupted]       │◀──close──────────│
//	    │◀──ErrorResponse(57014)│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    │  [HandleUnwatchAfterCancel]                 │                  │
//	    │  [cancelState: sent -> idle]                │                  │
//	    │───Query(;)───────────▶│ [drain]             │                  │
//	    │◀──EmptyQueryResponse──│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainExecCanceled(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	ctx, cleanup := cancelOnActive(t, observer, pid)
	defer cleanup()

	_, err = pgConn.Exec(ctx, "SELECT 1, "+pgSleepBlock).ReadAll()
	require.Error(t, err)

	ensureConnValid(t, pgConn)
}

// Scenario 1 variant: same flow as TestCancelAndDrainExecCanceled but exercises the extended query
// protocol path (ExecParams -> Parse/Bind/Describe/Execute instead of simple Query).
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Parse/Bind/Exec────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │  [ctx cancelled]      │                     │                  │
//	    │                       │                     │──CancelReq──────▶│
//	    │                       │                     │                  │──SIGINT──▶│
//	    │                       │ [interrupted]       │◀──close──────────│
//	    │◀──ErrorResponse(57014)│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    │  [HandleUnwatchAfterCancel]                 │                  │
//	    │  [cancelState: sent -> idle]                │                  │
//	    │───Query(;)───────────▶│ [drain]             │                  │
//	    │◀──EmptyQueryResponse──│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainExecParamsCanceled(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	ctx, cleanup := cancelOnActive(t, observer, pid)
	defer cleanup()

	result := pgConn.ExecParams(ctx, "SELECT 1, "+pgSleepBlock, nil, nil, nil, nil)
	_, err = result.Close()
	require.Error(t, err)

	ensureConnValid(t, pgConn)
}

// Scenario 1 variant: same flow exercised through the COPY TO protocol path.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(COPY...)─────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │  [ctx cancelled]      │                     │                  │
//	    │                       │                     │──CancelReq──────▶│
//	    │                       │                     │                  │──SIGINT──▶│
//	    │                       │ [interrupted]       │◀──close──────────│
//	    │◀──ErrorResponse(57014)│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    │  [HandleUnwatchAfterCancel]                 │                  │
//	    │  [cancelState: sent -> idle]                │                  │
//	    │───Query(;)───────────▶│ [drain]             │                  │
//	    │◀──EmptyQueryResponse──│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainCopyToCanceled(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	ctx, cleanup := cancelOnActive(t, observer, pid)
	defer cleanup()

	_, err = pgConn.CopyTo(ctx, nil, "COPY (SELECT "+pgSleepBlock+") TO STDOUT")
	require.Error(t, err)

	ensureConnValid(t, pgConn)
}

// Scenario 1 followed by a Prepare: after the cancel+drain cycle cleans up, the extended query
// protocol (Prepare -> Parse + DescribeStatement) works on the same connection.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(sql)─────────▶│                     │                  │
//	    │  [ctx cancelled, cancel, drain]             │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
//	    │───Parse/Describe─────▶│                     │                  │
//	    │◀──ParseComplete───────│                     │                  │
//	    │◀──ParameterDescription│                     │                  │
//	    │◀──RowDescription──────│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    ▼ prepare succeeded (ok)▼                     ▼                  ▼
func TestCancelAndDrainPrepareSurvivesCancelCycle(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	ctx, cleanup := cancelOnActive(t, observer, pid)
	defer cleanup()

	_, err = pgConn.Exec(ctx, "SELECT "+pgSleepBlock).ReadAll()
	require.Error(t, err)

	ensureConnValid(t, pgConn)

	sd, err := pgConn.Prepare(t.Context(), "test_stmt", "SELECT 1", nil)
	require.NoError(t, err)
	require.NotNil(t, sd)
}

// Scenario 3: Single-";" drain absorbs the stale 57014
//
// Same race as scenario 2 (the bug), but HandleUnwatchAfterCancel sends exactly one ";" to flush
// the pending cancel before the connection is reused. This test runs 50 cancel+query cycles and
// verifies that no 57014 ever bleeds into the subsequent query.
//
// One ";" is sufficient because PostgreSQL sets QueryCancelPending at most once per cancel signal.
// After the 57014 is raised and sent, the flag is cleared. There is no mechanism for a second
// 57014 from the same cancel.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(sql)─────────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │  [ctx cancelled]      │                     │                  │
//	    │◀──CommandComplete─────│                     │                  │
//	    │◀──ReadyForQuery───────│ [idle]              │                  │
//	    │                       │                     │──CancelReq──────▶│
//	    │                       │                     │                  │──SIGINT──▶│
//	    │                       │ [QueryCancelPending]│◀──close──────────│
//	    │                       │                     │                  │
//	    │  [HandleUnwatchAfterCancel]                 │                  │
//	    │  [cancelState: sent -> idle]                │                  │
//	    │───Query(;)───────────▶│ [drain]             │                  │
//	    │◀──ErrorResponse(57014)│ [flag consumed]     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    │───Query(next sql)────▶│ [clean]             │                  │
//	    │◀──CommandComplete─────│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainNoStale57014Bleed(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	for i := range 50 {
		ctx, cleanup := cancelOnActive(t, observer, pid)
		pgConn.Exec(ctx, "SELECT "+pgSleepBlock).ReadAll()
		cleanup()

		result := pgConn.ExecParams(
			t.Context(),
			"SELECT $1::TEXT",
			[][]byte{[]byte(fmt.Sprintf("iter_%d", i))},
			nil, nil, nil,
		).Read()
		require.NoError(t, result.Err, "iteration %d: stale cancel leaked into next query", i)
		require.Equal(t, 1, len(result.Rows))
		require.Equal(t, fmt.Sprintf("iter_%d", i), string(result.Rows[0][0]))
	}
}

// Scenario 1 / Scenario 3 repeated: exercises multiple cancel+drain cycles on the same connection
// to verify that the state machine resets cleanly each time.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │                       │                     │                  │
//	    │  [repeat 20x: ctx cancelled -> cancel -> drain -> ensureConnValid]
//	    │                       │                     │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainConnectionReuseCycles(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	for range 20 {
		ctx, cleanup := cancelOnActive(t, observer, pid)
		pgConn.Exec(ctx, "SELECT "+pgSleepBlock).ReadAll()
		cleanup()
		ensureConnValid(t, pgConn)
	}
}

// No-cancel path: the query completes before the context deadline. The context watcher never fires
// HandleCancel. No cancel request is sent, no drain is needed. This is the steady-state happy path
// and must not regress.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(sql)─────────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │◀──RowDescription──────│                     │                  │
//	    │◀──DataRow─────────────│                     │                  │
//	    │◀──CommandComplete─────│                     │                  │
//	    │◀──ReadyForQuery───────│                     │                  │
//	    │                       │                     │                  │
//	    │ [ctx not cancelled -- no cancel, no drain]  │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainQueryCompletesBeforeCancel(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	results, err := pgConn.Exec(t.Context(), "SELECT 42").ReadAll()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "42", string(results[0].Rows[0][0]))

	ensureConnValid(t, pgConn)
}

// Scenario 5: Duplicate cancel (mutex prevents double-send)
//
// CancelRequest is called twice on the same connection while a query is running. The first call
// transitions idle -> inFlight -> sent. The second call sees cancelStateSent and returns nil
// immediately -- no second cancel packet is sent, so at most one 57014 is produced.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(sql)─────────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │                       │                     │                  │
//	    │  [CancelRequest #1: idle -> inFlight (ok)]  │                  │
//	    │                       │                     │──CancelReq──────▶│
//	    │  [CancelRequest #1: inFlight -> sent]       │                  │
//	    │                       │                     │                  │
//	    │  [CancelRequest #2: state == sent -> no-op] │                  │
//	    │                       │                     │                  │
//	    ▼ only one cancel sent  ▼                     ▼                  ▼
func TestCancelAndDrainCancelRequestIdempotent(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	multiResult := pgConn.Exec(t.Context(), "SELECT "+pgSleepBlock)

	waitUntilActive(t, t.Context(), observer, pid)

	err = pgConn.CancelRequest(t.Context())
	require.NoError(t, err)

	err = pgConn.CancelRequest(t.Context())
	require.NoError(t, err)

	for multiResult.NextResult() {
	}
	err = multiResult.Close()
	require.Error(t, err)

	ensureConnValid(t, pgConn)
}

// Scenario 5 variant: multiple goroutines race to call CancelRequest concurrently. The mutex
// ensures only one caller transitions idle -> inFlight -> sent. All others either block on the
// in-flight done context and return nil, or see cancelStateSent and return nil immediately.
// Either way, exactly one cancel packet is sent.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │───Query(sql)─────────▶│                     │                  │
//	    │                       │ [executing]         │                  │
//	    │ [ctx cancelled]       │                     │                  │
//	    │ [goroutine 1: idle -> inFlight (ok)]        │                  │
//	    │                       │                     │──CancelReq──────▶│
//	    │                       │                     │                  │
//	    │ [goroutines 2-5: inFlight -> block on done] │                  │
//	    │                       │                     │                  │
//	    │ [goroutine 1 completes -> sent, doneFn()]   │                  │
//	    │ [goroutines 2-5 unblock -> return nil]      │                  │
//	    │                       │                     │                  │
//	    ▼ only one cancel sent  ▼                     ▼                  ▼
func TestCancelAndDrainConcurrentCancelRequest(t *testing.T) {
	t.Parallel()

	config := buildCancelAndDrainConfig(t)
	pgConn, err := pgconn.ConnectConfig(t.Context(), config)
	require.NoError(t, err)
	defer closeConn(t, pgConn)

	if pgConn.ParameterStatus("crdb_version") != "" {
		t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
	}

	observer := newObserver(t)
	pid := getBackendPID(t, pgConn)

	multiResult := pgConn.Exec(t.Context(), "SELECT "+pgSleepBlock)

	waitUntilActive(t, t.Context(), observer, pid)

	const goroutines = 5
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make([]error, goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = pgConn.CancelRequest(t.Context())
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d", i)
	}

	for multiResult.NextResult() {
	}
	err = multiResult.Close()
	require.Error(t, err)

	ensureConnValid(t, pgConn)
}

// Stress test: 10 parallel connections x 20 cancel+drain cycles each. Exercises the full
// cancel -> drain -> reuse path under contention, covering scenarios 1 and 3 in aggregate.
// The connection must remain valid after every cycle.
//
//	Client (conn A)            Server Backend        Client (conn B)   Postmaster
//	    │                       │                     │                  │
//	    │  [repeat 20x: cancel on active -> drain -> ensureConnValid]
//	    │                       │                     │                  │
//	    ▼ connection clean (ok) ▼                     ▼                  ▼
func TestCancelAndDrainStress(t *testing.T) {
	t.Parallel()

	for i := range 10 {
		t.Run(fmt.Sprintf("Worker %d", i), func(t *testing.T) {
			t.Parallel()

			config := buildCancelAndDrainConfig(t)
			pgConn, err := pgconn.ConnectConfig(t.Context(), config)
			require.NoError(t, err)
			defer closeConn(t, pgConn)

			if pgConn.ParameterStatus("crdb_version") != "" {
				t.Skip("CockroachDB incompatible with PostgreSQL: pg_stat_activity")
			}

			observer := newObserver(t)
			pid := getBackendPID(t, pgConn)

			for range 20 {
				ctx, cleanup := cancelOnActive(t, observer, pid)
				pgConn.Exec(ctx, "SELECT 1, "+pgSleepBlock).ReadAll()
				cleanup()
				ensureConnValid(t, pgConn)
			}
		})
	}
}
