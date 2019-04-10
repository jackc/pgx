package pool

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx"
)

type Tx struct {
	t *pgx.Tx
	c *Conn
}

func (tx *Tx) Commit() error {
	err := tx.t.Commit()
	if tx.c != nil {
		tx.c.Release()
		tx.c = nil
	}
	return err
}

func (tx *Tx) Rollback() error {
	err := tx.t.Rollback()
	if tx.c != nil {
		tx.c.Release()
		tx.c = nil
	}
	return err
}

func (tx *Tx) Err() error {
	return tx.t.Err()
}

func (tx *Tx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return tx.c.Exec(ctx, sql, arguments...)
}

func (tx *Tx) Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (*Rows, error) {
	return tx.c.Query(ctx, sql, optionsAndArgs...)
}

func (tx *Tx) QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) *Row {
	return tx.c.QueryRow(ctx, sql, optionsAndArgs...)
}
