package pgx

import (
	"bytes"
	"errors"
	"fmt"
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
	TxStatusCommitSuccess   = 1
	TxStatusRollbackSuccess = 2
)

type TxOptions struct {
	IsoLevel       TxIsoLevel
	AccessMode     TxAccessMode
	DeferrableMode TxDeferrableMode
}

var ErrTxClosed = errors.New("tx is closed")

// ErrTxCommitRollback occurs when an error has occurred in a transaction and
// Commit() is called. PostgreSQL accepts COMMIT on aborted transactions, but
// it is treated as ROLLBACK.
var ErrTxCommitRollback = errors.New("commit unexpectedly resulted in rollback")

// Begin starts a transaction with the default transaction mode for the
// current connection. To use a specific transaction mode see BeginEx.
func (c *Conn) Begin() (*Tx, error) {
	return c.BeginEx(nil)
}

// BeginEx starts a transaction with txOptions determining the transaction
// mode.
func (c *Conn) BeginEx(txOptions *TxOptions) (*Tx, error) {
	var beginSQL string
	if txOptions == nil {
		beginSQL = "begin"
	} else {
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

		beginSQL = buf.String()
	}

	_, err := c.Exec(beginSQL)
	if err != nil {
		return nil, err
	}

	return &Tx{conn: c}, nil
}

// Tx represents a database transaction.
//
// All Tx methods return ErrTxClosed if Commit or Rollback has already been
// called on the Tx.
type Tx struct {
	conn       *Conn
	afterClose func(*Tx)
	err        error
	status     int8
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.status != TxStatusInProgress {
		return ErrTxClosed
	}

	commandTag, err := tx.conn.Exec("commit")
	if err == nil && commandTag == "COMMIT" {
		tx.status = TxStatusCommitSuccess
	} else if err == nil && commandTag == "ROLLBACK" {
		tx.status = TxStatusCommitFailure
		tx.err = ErrTxCommitRollback
	} else {
		tx.status = TxStatusCommitFailure
		tx.err = err
	}

	if tx.afterClose != nil {
		tx.afterClose(tx)
	}
	return tx.err
}

// Rollback rolls back the transaction. Rollback will return ErrTxClosed if the
// Tx is already closed, but is otherwise safe to call multiple times. Hence, a
// defer tx.Rollback() is safe even if tx.Commit() will be called first in a
// non-error condition.
func (tx *Tx) Rollback() error {
	if tx.status != TxStatusInProgress {
		return ErrTxClosed
	}

	_, tx.err = tx.conn.Exec("rollback")
	if tx.err == nil {
		tx.status = TxStatusRollbackSuccess
	} else {
		tx.status = TxStatusRollbackFailure
	}

	if tx.afterClose != nil {
		tx.afterClose(tx)
	}
	return tx.err
}

// Exec delegates to the underlying *Conn
func (tx *Tx) Exec(sql string, arguments ...interface{}) (commandTag CommandTag, err error) {
	if tx.status != TxStatusInProgress {
		return CommandTag(""), ErrTxClosed
	}

	return tx.conn.Exec(sql, arguments...)
}

// Prepare delegates to the underlying *Conn
func (tx *Tx) Prepare(name, sql string) (*PreparedStatement, error) {
	return tx.PrepareEx(name, sql, nil)
}

// PrepareEx delegates to the underlying *Conn
func (tx *Tx) PrepareEx(name, sql string, opts *PrepareExOptions) (*PreparedStatement, error) {
	if tx.status != TxStatusInProgress {
		return nil, ErrTxClosed
	}

	return tx.conn.PrepareEx(name, sql, opts)
}

// Query delegates to the underlying *Conn
func (tx *Tx) Query(sql string, args ...interface{}) (*Rows, error) {
	if tx.status != TxStatusInProgress {
		// Because checking for errors can be deferred to the *Rows, build one with the error
		err := ErrTxClosed
		return &Rows{closed: true, err: err}, err
	}

	return tx.conn.Query(sql, args...)
}

// QueryRow delegates to the underlying *Conn
func (tx *Tx) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := tx.Query(sql, args...)
	return (*Row)(rows)
}

// CopyTo delegates to the underlying *Conn
func (tx *Tx) CopyTo(tableName string, columnNames []string, rowSrc CopyToSource) (int, error) {
	if tx.status != TxStatusInProgress {
		return 0, ErrTxClosed
	}

	return tx.conn.CopyTo(tableName, columnNames, rowSrc)
}

// Conn returns the *Conn this transaction is using.
func (tx *Tx) Conn() *Conn {
	return tx.conn
}

// Status returns the status of the transaction from the set of
// pgx.TxStatus* constants.
func (tx *Tx) Status() int8 {
	return tx.status
}

// Err returns the final error state, if any, of calling Commit or Rollback.
func (tx *Tx) Err() error {
	return tx.err
}

// AfterClose adds f to a LILO queue of functions that will be called when
// the transaction is closed (either Commit or Rollback).
func (tx *Tx) AfterClose(f func(*Tx)) {
	if tx.afterClose == nil {
		tx.afterClose = f
	} else {
		prevFn := tx.afterClose
		tx.afterClose = func(tx *Tx) {
			f(tx)
			prevFn(tx)
		}
	}
}
