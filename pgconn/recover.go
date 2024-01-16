package pgconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func (pgConn *PgConn) asyncRecover(ctx context.Context) {
	go func() {
		err := pgConn.recoverConnection(ctx)
		if err != nil {
			// Close timeout
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_ = pgConn.Close(ctx)
		}
	}()
}

func (pgConn *PgConn) recoverConnection(ctx context.Context) error {
	if pgConn.status != connStatusBusy {
		return nil
	}

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
	if err := pgConn.recoverSocket(); err != nil {
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

	if pgConn.txStatus != 'I' {
		if err := pgConn.execLockedRoundTrip(ctx, "ROLLBACK;"); err != nil {
			return fmt.Errorf("rollback: %w", err)
		}
	}

	pgConn.resetConn()

	return nil
}

func (pgConn *PgConn) execLocked(ctx context.Context, sql string) *MultiResultReader {
	pgConn.multiResultReader = MultiResultReader{
		pgConn: pgConn,
		ctx:    ctx,
	}
	multiResult := &pgConn.multiResultReader
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
		pgConn.asyncClose(err)
		pgConn.contextWatcher.Unwatch()
		multiResult.closed = true
		multiResult.err = err
		return multiResult
	}

	return multiResult
}

func (pgConn *PgConn) execLockedRoundTrip(ctx context.Context, sql string) error {
	mrr := pgConn.execLocked(ctx, sql)

	for !mrr.closed && mrr.err == nil {
		_, err := mrr.receiveMessage()
		if err != nil {
			return mrr.err
		}
	}

	return mrr.err
}

func (pgConn *PgConn) recoverSocket() error {
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
			if pgErr.Code == "57014" {
				return nil
			}
			return pgErr
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
	pgConn.status = connStatusIdle
	pgConn.cleanupWithoutReset = false
	pgConn.contextWatcher.Unwatch()
}

func (pgConn *PgConn) cleanSocket() error {
	// If this option is set, then there is nothing
	// to read from the socket and there is no need
	// to hang forever on reading.
	if pgConn.cleanupWithoutReset {
		return nil
	}

	// Read all the data left from the previous request.
	if err := pgConn.recoverSocket(); err != nil {
		return fmt.Errorf("recoverSocket: %w", err)
	}

	return nil
}

func (pgConn *PgConn) execRoundTrip(ctx context.Context, sql string) error {
	rr := pgConn.Exec(ctx, sql)
	if rr.err != nil {
		return fmt.Errorf("exec: %w", rr.err)
	}

	// reading all that is left in socket
	if err := rr.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

func (pgConn *PgConn) cleanup(ctx context.Context, resetQuery string) error {
	deadline, ok := ctx.Deadline()
	if ok {
		pgConn.conn.SetDeadline(deadline)
	} else {
		pgConn.conn.SetDeadline(time.Time{})
	}

	if err := pgConn.cleanSocket(); err != nil {
		return fmt.Errorf("clean socket: %w", err)
	}

	// Switch status to idle to not receive an error
	// while locking connection on exec operation.
	pgConn.status = connStatusIdle

	// Rollback if there is an active transaction,
	// Checking TxStatus to prevent overhead
	if pgConn.TxStatus() != 'I' {
		if err := pgConn.execRoundTrip(ctx, "ROLLBACK;"); err != nil {
			return fmt.Errorf("rollback: %w", err)
		}
	}

	// Full session recoverSocket
	if resetQuery != "" {
		if err := pgConn.execRoundTrip(ctx, resetQuery); err != nil {
			return fmt.Errorf("discard all: %w", err)
		}
	}

	// Reset everything.
	pgConn.peekedMsg = nil
	pgConn.bufferingReceiveMsg = nil
	pgConn.bufferingReceiveErr = nil
	pgConn.bufferingReceive = false
	pgConn.slowWriteTimer.Stop()
	pgConn.conn.SetDeadline(time.Time{})
	pgConn.status = connStatusIdle
	pgConn.cleanupWithoutReset = false
	pgConn.contextWatcher.Unwatch()

	return nil
}

func (pgConn *PgConn) LaunchCleanup(ctx context.Context, resetQuery string, onCleanupSucceeded func(), onCleanupFailed func(error)) (cleanupLaunched bool) {
	if pgConn.status != connStatusNeedCleanup {
		return false
	}

	go func() {
		if err := pgConn.cleanup(ctx, resetQuery); err != nil {
			if onCleanupFailed != nil {
				onCleanupFailed(err)
			}

			return
		}

		if onCleanupSucceeded != nil {
			onCleanupSucceeded()
		}
	}()

	return true
}

func (pgConn *PgConn) setCleanupNeeded(reason error) bool {
	if pgConn.status != connStatusBusy {
		return false
	}

	// Set a connStatusNeedCleanup status to recoverSocket connection later.
	var netErr net.Error
	if isNetErr := errors.As(reason, &netErr); isNetErr && netErr.Timeout() {
		pgConn.status = connStatusNeedCleanup

		// if there was no data send to the server, then there is nothing to read back
		// so we dont need to recoverSocket
		if SafeToRetry(reason) {
			pgConn.cleanupWithoutReset = true
		}

		return true
	}

	return false
}
