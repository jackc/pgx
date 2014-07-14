package pgx

import (
	"errors"
	"fmt"
)

// Transaction isolation levels
const (
	Serializable    = "serializable"
	RepeatableRead  = "repeatable read"
	ReadCommitted   = "read committed"
	ReadUncommitted = "read uncommitted"
)

var ErrTxClosed = errors.New("tx is closed")

// Begin starts a transaction with the default isolation level for the current
// connection. To use a specific isolation level see BeginIso.
func (c *Conn) Begin() (*Tx, error) {
	return c.begin("")
}

// BeginIso starts a transaction with isoLevel as the transaction isolation
// level.
//
// Valid isolation levels (and their constants) are:
//   serializable (pgx.Serializable)
//   repeatable read (pgx.RepeatableRead)
//   read committed (pgx.ReadCommitted)
//   read uncommitted (pgx.ReadUncommitted)
func (c *Conn) BeginIso(isoLevel string) (*Tx, error) {
	return c.begin(isoLevel)
}

func (c *Conn) begin(isoLevel string) (*Tx, error) {
	var beginSql string
	if isoLevel == "" {
		beginSql = "begin"
	} else {
		beginSql = fmt.Sprintf("begin isolation level %s", isoLevel)
	}

	_, err := c.Exec(beginSql)
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
	pool   *ConnPool
	conn   *Conn
	closed bool
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.closed {
		return ErrTxClosed
	}

	_, err := tx.conn.Exec("commit")
	tx.close()
	return err
}

// Rollback rolls back the transaction. Rollback will return ErrTxClosed if the
// Tx is already closed, but is otherwise safe to call multiple times. Hence, a
// defer tx.Rollback() is safe even if tx.Commit() will be called first in a
// non-error condition.
func (tx *Tx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}

	_, err := tx.conn.Exec("rollback")
	tx.close()
	return err
}

func (tx *Tx) close() {
	if tx.pool != nil {
		tx.pool.Release(tx.conn)
		tx.pool = nil
	}
	tx.closed = true
}

// Exec delegates to the underlying *Conn
func (tx *Tx) Exec(sql string, arguments ...interface{}) (commandTag CommandTag, err error) {
	if tx.closed {
		return CommandTag(""), ErrTxClosed
	}

	return tx.conn.Exec(sql, arguments...)
}

// Query delegates to the underlying *Conn
func (tx *Tx) Query(sql string, args ...interface{}) (*Rows, error) {
	if tx.closed {
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
