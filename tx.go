package pgx

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	errors "golang.org/x/xerrors"
)

type TxIsoLevel string

// Transaction isolation levels
const (
	Serializable    = TxIsoLevel("serializable")
	RepeatableRead  = TxIsoLevel("repeatable read")
	ReadCommitted   = TxIsoLevel("read committed")
	ReadUncommitted = TxIsoLevel("read uncommitted")
)

type TxAccessMode string

// Transaction access modes
const (
	ReadWrite = TxAccessMode("read write")
	ReadOnly  = TxAccessMode("read only")
)

type TxDeferrableMode string

// Transaction deferrable modes
const (
	Deferrable    = TxDeferrableMode("deferrable")
	NotDeferrable = TxDeferrableMode("not deferrable")
)

const (
	TxStatusInProgress      = 0
	TxStatusCommitFailure   = -1
	TxStatusRollbackFailure = -2
	TxStatusInFailure       = -3
	TxStatusCommitSuccess   = 1
	TxStatusRollbackSuccess = 2
)

type TxOptions struct {
	IsoLevel       TxIsoLevel
	AccessMode     TxAccessMode
	DeferrableMode TxDeferrableMode
}

func (txOptions *TxOptions) beginSQL() string {
	if txOptions == nil {
		return "begin"
	}

	buf := &bytes.Buffer{}
	buf.WriteString("begin")
	if txOptions.IsoLevel != "" {
		fmt.Fprintf(buf, " isolation level %s", txOptions.IsoLevel)
	}
	if txOptions.AccessMode != "" {
		fmt.Fprintf(buf, " %s", txOptions.AccessMode)
	}
	if txOptions.DeferrableMode != "" {
		fmt.Fprintf(buf, " %s", txOptions.DeferrableMode)
	}

	return buf.String()
}

var ErrTxClosed = errors.New("tx is closed")
var ErrTxInFailure = errors.New("tx failed")

// ErrTxCommitRollback occurs when an error has occurred in a transaction and
// Commit() is called. PostgreSQL accepts COMMIT on aborted transactions, but
// it is treated as ROLLBACK.
var ErrTxCommitRollback = errors.New("commit unexpectedly resulted in rollback")

// BeginEx starts a transaction with txOptions determining the transaction mode. txOptions can be nil. Unlike
// database/sql, the context only affects the begin command. i.e. there is no auto-rollback on context cancelation.
func (c *Conn) Begin(ctx context.Context, txOptions *TxOptions) (*Tx, error) {
	_, err := c.Exec(ctx, txOptions.beginSQL())
	if err != nil {
		// begin should never fail unless there is an underlying connection issue or
		// a context timeout. In either case, the connection is possibly broken.
		c.die(errors.New("failed to begin transaction"))
		return nil, err
	}

	return &Tx{conn: c}, nil
}

// Tx represents a database transaction.
//
// All Tx methods return ErrTxClosed if Commit or Rollback has already been
// called on the Tx.
type Tx struct {
	conn   *Conn
	err    error
	status int8
}

// Commit commits the transaction.
func (tx *Tx) Commit(ctx context.Context) error {
	if tx.status != TxStatusInProgress {
		return ErrTxClosed
	}

	commandTag, err := tx.conn.Exec(ctx, "commit")
	if err == nil && string(commandTag) == "COMMIT" {
		tx.status = TxStatusCommitSuccess
	} else if err == nil && string(commandTag) == "ROLLBACK" {
		tx.status = TxStatusCommitFailure
		tx.err = ErrTxCommitRollback
	} else {
		tx.status = TxStatusCommitFailure
		tx.err = err
		// A commit failure leaves the connection in an undefined state
		tx.conn.die(errors.New("commit failed"))
	}

	return tx.err
}

// Rollback rolls back the transaction. Rollback will return ErrTxClosed if the
// Tx is already closed, but is otherwise safe to call multiple times. Hence, a
// defer tx.Rollback() is safe even if tx.Commit() will be called first in a
// non-error condition.
func (tx *Tx) Rollback(ctx context.Context) error {
	if tx.status != TxStatusInProgress {
		return ErrTxClosed
	}

	_, tx.err = tx.conn.Exec(ctx, "rollback")
	if tx.err == nil {
		tx.status = TxStatusRollbackSuccess
	} else {
		tx.status = TxStatusRollbackFailure
		// A rollback failure leaves the connection in an undefined state
		tx.conn.die(errors.New("rollback failed"))
	}

	return tx.err
}

// Exec delegates to the underlying *Conn
func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return tx.conn.Exec(ctx, sql, arguments...)
}

// Prepare delegates to the underlying *Conn
func (tx *Tx) Prepare(ctx context.Context, name, sql string) (*PreparedStatement, error) {
	if tx.status != TxStatusInProgress {
		return nil, ErrTxClosed
	}

	return tx.conn.Prepare(ctx, name, sql)
}

// Query delegates to the underlying *Conn
func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	if tx.status != TxStatusInProgress {
		// Because checking for errors can be deferred to the *Rows, build one with the error
		err := ErrTxClosed
		return &connRows{closed: true, err: err}, err
	}

	return tx.conn.Query(ctx, sql, args...)
}

// QueryRow delegates to the underlying *Conn
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	rows, _ := tx.Query(ctx, sql, args...)
	return (*connRow)(rows.(*connRows))
}

// CopyFrom delegates to the underlying *Conn
func (tx *Tx) CopyFrom(ctx context.Context, tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int64, error) {
	if tx.status != TxStatusInProgress {
		return 0, ErrTxClosed
	}

	return tx.conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// SendBatch delegates to the underlying *Conn
func (tx *Tx) SendBatch(ctx context.Context, b *Batch) BatchResults {
	if tx.status != TxStatusInProgress {
		return &batchResults{err: ErrTxClosed}
	}

	return tx.conn.SendBatch(ctx, b)
}

// Status returns the status of the transaction from the set of
// pgx.TxStatus* constants.
func (tx *Tx) Status() int8 {
	if tx.status == TxStatusInProgress && tx.conn.pgConn.TxStatus == 'E' {
		return TxStatusInFailure
	}
	return tx.status
}

// Err returns the final error state, if any, of calling Commit or Rollback.
func (tx *Tx) Err() error {
	return tx.err
}
