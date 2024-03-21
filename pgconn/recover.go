package pgconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func (pgConn *PgConn) WaitForRecover() {
	pgConn.recoverWg.Wait()
}

func (pgConn *PgConn) handleConnectionError(reason error) {
	var netErr net.Error
	if isNetErr := errors.As(reason, &netErr); isNetErr && netErr.Timeout() {
		pgConn.asyncRecover()
	} else {
		pgConn.asyncClose()
	}
}

func (pgConn *PgConn) recoverContext() (recoverCtx context.Context, recoverCancel context.CancelFunc) {
	ctx := context.Background()

	if pgConn.config.RecoverTimeout != 0 {
		recoverCtx, recoverCancel = context.WithTimeout(ctx, pgConn.config.RecoverTimeout)
	} else {
		recoverCtx = ctx
		recoverCancel = func() {}
	}

	return
}

func (pgConn *PgConn) asyncRecover() {
	if pgConn.status.Load() != connStatusBusy {
		return
	}
	pgConn.status.Store(connStatusRecovering)
	pgConn.recoverWg.Add(1)

	go func() {
		defer pgConn.recoverWg.Done()

		recoverCtx, recoverCancel := pgConn.recoverContext()
		defer recoverCancel()

		err := pgConn.recoverConnection(recoverCtx)
		if err == nil {
			pgConn.resetConn()
			return
		}
		// Close timeout
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_ = pgConn.Close(closeCtx)
	}()
}

func (pgConn *PgConn) recoverConnection(ctx context.Context) error {
	onRecoverCh := make(chan error, 1)
	if pgConn.config.OnRecover != nil {
		go func() {
			onRecoverCh <- pgConn.config.OnRecover(ctx, pgConn)
		}()
	}

	deadline, ok := ctx.Deadline()
	if ok {
		pgConn.conn.SetDeadline(deadline)
	} else {
		pgConn.conn.SetDeadline(time.Time{})
	}

	// Read all the data left from the previous request.
	if err := pgConn.recoverSocket(true); err != nil {
		return fmt.Errorf("recover socket: %w", err)
	}

	if pgConn.config.OnRecover != nil {
		select {
		case err := <-onRecoverCh:
			if err != nil {
				return fmt.Errorf("on recover: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if pgConn.txStatus == 'I' {
		return nil
	}

	// if connection is in transaction state we should rollback it
	if err := pgConn.execLockedRoundTrip(ctx, "ROLLBACK;"); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	return nil
}

func (pgConn *PgConn) execLocked(ctx context.Context, sql string) *MultiResultReader {
	multiResult := &MultiResultReader{
		pgConn: pgConn,
		ctx:    ctx,
	}
	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			multiResult.closed = true
			multiResult.err = newContextAlreadyDoneError(ctx)
			return multiResult
		default:
		}
		pgConn.contextWatcher.Watch(ctx)
	}

	pgConn.frontend.SendQuery(&pgproto3.Query{String: sql})
	err := pgConn.flushWithPotentialWriteReadDeadlock()
	if err != nil {
		pgConn.asyncClose()
		pgConn.contextWatcher.Unwatch()
		multiResult.closed = true
		multiResult.err = err
		return multiResult
	}

	return multiResult
}

func (pgConn *PgConn) execLockedRoundTrip(ctx context.Context, sql string) error {
	mr := pgConn.execLocked(ctx, sql)
	return mr.pgConn.recoverSocket(false)
}

func (pgConn *PgConn) recoverSocket(ignoreCancel bool) error {
	for {
		msg, err := pgConn.peekMessage()
		if err != nil {
			return fmt.Errorf("peek message: %w", err)
		}
		pgConn.peekedMsg = nil

		// every request is ended with ReadyForQuery message
		// so we just read till it comes or error occurs
		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			pgConn.txStatus = msg.TxStatus
			return nil
		case *pgproto3.ErrorResponse:
			pgErr := ErrorResponseToPgError(msg)
			// 57014 is code that indicates that query was canceled due to user request
			// we just ignore error as it is expected
			if !ignoreCancel || pgErr.Code != "57014" {
				return pgErr
			}
		}
	}
}

func (pgConn *PgConn) resetConn() {
	pgConn.peekedMsg = nil
	pgConn.bufferingReceiveMsg = nil
	pgConn.bufferingReceiveErr = nil
	pgConn.bufferingReceive = false
	pgConn.slowWriteTimer.Stop()
	pgConn.conn.SetDeadline(time.Time{})
	pgConn.contextWatcher.Unwatch()

	pgConn.unlock()
}
