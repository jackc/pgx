package pgx

import (
	"errors"
	"fmt"
)

func (c *Conn) Begin() (*Tx, error) {
	return c.begin("")
}

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

type Tx struct {
	pool   *ConnPool
	conn   *Conn
	closed bool
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.closed {
		return errors.New("tx is closed")
	}

	_, err := tx.conn.Exec("commit")
	tx.close()
	return err
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	if tx.closed {
		return errors.New("tx is closed")
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
		return CommandTag(""), errors.New("tx is closed")
	}

	return tx.conn.Exec(sql, arguments...)
}

// Query delegates to the underlying *Conn
func (tx *Tx) Query(sql string, args ...interface{}) (*Rows, error) {
	if tx.closed {
		// Because checking for errors can be deferred to the *Rows, build one with the error
		err := errors.New("tx is closed")
		return &Rows{closed: true, err: err}, err
	}

	return tx.conn.Query(sql, args...)
}

// QueryRow delegates to the underlying *Conn
func (tx *Tx) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := tx.conn.Query(sql, args...)
	return (*Row)(rows)
}

// Transaction runs f in a transaction. f should return true if the transaction
// should be committed or false if it should be rolled back. Return value committed
// is if the transaction was committed or not. committed should be checked separately
// from err as an explicit rollback is not an error. Transaction will use the default
// isolation level for the current connection. To use a specific isolation level see
// TransactionIso
func (c *Conn) Transaction(f func() bool) (committed bool, err error) {
	return c.transaction("", f)
}

// TransactionIso is the same as Transaction except it takes an isoLevel argument that
// it uses as the transaction isolation level.
//
// Valid isolation levels (and their constants) are:
//   serializable (pgx.Serializable)
//   repeatable read (pgx.RepeatableRead)
//   read committed (pgx.ReadCommitted)
//   read uncommitted (pgx.ReadUncommitted)
func (c *Conn) TransactionIso(isoLevel string, f func() bool) (committed bool, err error) {
	return c.transaction(isoLevel, f)
}

func (c *Conn) transaction(isoLevel string, f func() bool) (committed bool, err error) {
	var beginSql string
	if isoLevel == "" {
		beginSql = "begin"
	} else {
		beginSql = fmt.Sprintf("begin isolation level %s", isoLevel)
	}

	if _, err = c.Exec(beginSql); err != nil {
		return
	}
	defer func() {
		if committed && c.TxStatus == 'T' {
			_, err = c.Exec("commit")
			if err != nil {
				committed = false
			}
		} else {
			_, err = c.Exec("rollback")
			committed = false
		}
	}()

	committed = f()
	return
}
