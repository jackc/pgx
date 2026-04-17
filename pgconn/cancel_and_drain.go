package pgconn

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	defaultDeadlineDelay = time.Second
	defaultDrainTimeout  = 5 * time.Second

	queryCanceledSQLStateCode = "57014"

	cancelStateIdle     = 0
	cancelStateInFlight = 1
	cancelStateSent     = 2
)

// CancelAndDrainContextWatcherHandler handles cancelled contexts by first sending a cancel request, then draining any
// pending SQLSTATE 57014 (query_canceled) with a single ";" round-trip.
//
// Correctness depends on at most one cancel request being in flight per connection at any time. Each cancel request
// causes the server to set QueryCancelPending, which produces exactly one 57014. If two cancel requests were sent,
// two 57014s could arrive -- the first absorbed by the drain, the second bleeding into the next real query. This
// invariant is enforced by [PgConn.CancelRequest]'s mutex-guarded state machine, which blocks concurrent callers
// until the in-flight cancel completes.
type CancelAndDrainContextWatcherHandler struct {
	Conn *PgConn

	// DeadlineDelay is a net.Conn deadline set when the context is cancelled, used as a fallback to unblock blocked
	// reads. Defaults to defaultDeadlineDelay (1s).
	DeadlineDelay time.Duration

	// DrainTimeout caps the single drain round-trip. Defaults to defaultDrainTimeout (5s).
	DrainTimeout time.Duration

	doneCtx context.Context //nolint:containedctx // synchronization primitive, not a request-scoped context
	doneFn  context.CancelFunc
	stopFn  context.CancelFunc
}

var _ ctxwatch.Handler = (*CancelAndDrainContextWatcherHandler)(nil)

func (h *CancelAndDrainContextWatcherHandler) deadlineDelay() time.Duration {
	if h.DeadlineDelay == 0 {
		return defaultDeadlineDelay
	}
	return h.DeadlineDelay
}

func (h *CancelAndDrainContextWatcherHandler) drainTimeout() time.Duration {
	if h.DrainTimeout == 0 {
		return defaultDrainTimeout
	}
	return h.DrainTimeout
}

// HandleCancel is called when the watched context is cancelled. It applies a net.Conn deadline as a fallback and fires
// a cancel request in a goroutine. Mutual exclusion (at most one cancel in flight) is enforced by
// [PgConn.CancelRequest], not here -- the ctxwatch.Handler interface does not permit a return value, but CancelRequest
// will block if another cancel is already in progress.
//
// The parent context is inherited (via WithoutCancel) so that values like trace IDs propagate into the cancel request
// without inheriting its already-fired cancellation.
func (h *CancelAndDrainContextWatcherHandler) HandleCancel(ctx context.Context) {
	baseCtx := context.WithoutCancel(ctx)
	cancelCtx, stop := context.WithCancel(baseCtx)
	h.stopFn = stop

	h.doneCtx, h.doneFn = context.WithCancel(context.Background())

	deadline := time.Now().Add(h.deadlineDelay())
	h.Conn.conn.SetDeadline(deadline)

	go func() {
		defer h.doneFn()
		reqCtx, cancel := context.WithDeadline(cancelCtx, deadline)
		defer cancel()
		h.Conn.CancelRequest(reqCtx)
	}()
}

// HandleUnwatchAfterCancel is called after the cancelled query returns. It waits for the cancel goroutine, clears the
// deadline, and -- if the cancel was successfully sent (cancelStateSent) -- sends exactly one ";" to absorb any pending
// 57014. Finally it transitions back to idle.
func (h *CancelAndDrainContextWatcherHandler) HandleUnwatchAfterCancel() {
	if h.stopFn != nil {
		h.stopFn()
	}
	if h.doneCtx != nil {
		<-h.doneCtx.Done()
	}
	h.Conn.conn.SetDeadline(time.Time{})
	h.doneCtx = nil
	h.doneFn = nil
	h.stopFn = nil

	h.Conn.cancelMu.Lock()
	needsDrain := h.Conn.cancelMu.state == cancelStateSent
	if needsDrain {
		h.Conn.cancelMu.state = cancelStateIdle
	}
	h.Conn.cancelMu.Unlock()

	if !h.Conn.IsClosed() && needsDrain {
		ctx, cancel := context.WithTimeout(context.Background(), h.drainTimeout())
		defer cancel()
		h.Conn.drainOnce(ctx)
	}
}

// drainOnce sends a single ";" and reads the response. If the server returns 57014, the cancel was still pending and is
// now consumed. If the server returns a clean EmptyQueryResponse, the cancel was already consumed by the original query.
// Either way the connection is clean after one round-trip -- no loop required.
//
// This design assumes at most one cancel is in flight per connection (enforced by [PgConn.CancelRequest]). A single
// cancel produces at most one QueryCancelPending flag on the server, which yields at most one 57014.
func (pgConn *PgConn) drainOnce(ctx context.Context) {
	if deadline, ok := ctx.Deadline(); ok {
		pgConn.conn.SetDeadline(deadline)
		defer pgConn.conn.SetDeadline(time.Time{})
	}

	pgConn.frontend.Send(&pgproto3.Query{String: ";"})
	if err := pgConn.frontend.Flush(); err != nil {
		pgConn.asyncClose()
		return
	}

	for {
		msg, err := pgConn.receiveMessage()
		if err != nil {
			pgConn.asyncClose()
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return
		case *pgproto3.ErrorResponse:
			pgErr := ErrorResponseToPgError(msg)
			if pgErr.Code != queryCanceledSQLStateCode {
				pgConn.asyncClose()
				return
			}
			// 57014 absorbed -- continue reading until ReadyForQuery
		case *pgproto3.EmptyQueryResponse:
			// Expected response for ";" -- continue reading until ReadyForQuery
		}
	}
}
