package pgconn

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
)

// CancelAndDrainContextWatcherHandler handles cancelled contexts by sending a cancel request to the server and then
// draining any pending SQLSTATE 57014 (query_canceled) with a single ";" round-trip. Unlike [CancelRequestContextWatcherHandler],
// no fixed sleep is used; the drain is deterministic.
type CancelAndDrainContextWatcherHandler struct {
	Conn *PgConn

	// DeadlineDelay is the network deadline set on the connection when the context
	// is cancelled, used as a fallback to unblock any blocked read. Defaults to 1s.
	DeadlineDelay time.Duration

	// DrainTimeout is the maximum time to spend draining a cancelled query's
	// in-flight results via SELECT 1 polling. Defaults to 5s.
	DrainTimeout time.Duration

	cancelFinishedChan chan struct{}
	stopFn             context.CancelFunc
}

var _ ctxwatch.Handler = (*CancelAndDrainContextWatcherHandler)(nil)

func (h *CancelAndDrainContextWatcherHandler) deadlineDelay() time.Duration {
	if h.DeadlineDelay == 0 {
		return time.Second
	}
	return h.DeadlineDelay
}

func (h *CancelAndDrainContextWatcherHandler) drainTimeout() time.Duration {
	if h.DrainTimeout == 0 {
		return 5 * time.Second
	}
	return h.DrainTimeout
}

// HandleCancel is called when the context is cancelled. It sets a net.Conn deadline
// as a fallback and sends a PostgreSQL cancel request in a goroutine.
func (h *CancelAndDrainContextWatcherHandler) HandleCancel(_ context.Context) {
	h.cancelFinishedChan = make(chan struct{})
	cancelCtx, stop := context.WithCancel(context.Background())
	h.stopFn = stop

	deadline := time.Now().Add(h.deadlineDelay())
	h.Conn.conn.SetDeadline(deadline)

	doneCh := h.cancelFinishedChan
	go func() {
		defer close(doneCh)
		reqCtx, cancel := context.WithDeadline(cancelCtx, deadline)
		defer cancel()
		h.Conn.CancelRequest(reqCtx)
	}()
}

// HandleUnwatchAfterCancel is called after the cancelled query returns. It stops the cancel goroutine (if still
// running), clears the net.Conn deadline, and drains any in-flight cancel by polling SELECT 1.
func (h *CancelAndDrainContextWatcherHandler) HandleUnwatchAfterCancel() {
	if h.stopFn != nil {
		h.stopFn()
	}
	if h.cancelFinishedChan != nil {
		<-h.cancelFinishedChan
	}
	h.Conn.conn.SetDeadline(time.Time{})
	h.cancelFinishedChan = nil
	h.stopFn = nil

	if !h.Conn.IsClosed() {
		ctx, cancel := context.WithTimeout(context.Background(), h.drainTimeout())
		defer cancel()
		h.Conn.execInternalForDrain(ctx)
	}
}

// queryCanceledSQLStateCode is SQLSTATE 57014 (query_canceled).
const queryCanceledSQLStateCode = "57014"

// execInternalForDrain sends a single ";" and reads until ReadyForQuery, absorbing any
// SQLSTATE 57014 (query_canceled). One round-trip is sufficient: PostgreSQL sets
// QueryCancelPending at most once per cancel signal, so at most one 57014 can arrive.
// On any failure the connection is asyncClosed.
//
// Called while the connection is still logically "busy" from pgconn's perspective
// (lock is held and contextWatcher.Unwatch has been called) but idle from the
// PostgreSQL server's perspective (ReadyForQuery was just received). This means
// it bypasses the normal lock/unlock and contextWatcher.Watch paths.
//
// The deadline from ctx is applied directly to the net.Conn.
func (pgConn *PgConn) execInternalForDrain(ctx context.Context) {
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
			// 57014 absorbed — continue reading until ReadyForQuery
		case *pgproto3.EmptyQueryResponse:
			// Expected response for ";".
		}
	}
}
