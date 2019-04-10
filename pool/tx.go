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

func (tx *Tx) Query(sql string, args ...interface{}) (*Rows, error) {
	return tx.c.Query(sql, args...)
}

func (tx *Tx) QueryEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (*Rows, error) {
	return tx.c.QueryEx(ctx, sql, options, args...)
}

func (tx *Tx) QueryRow(sql string, args ...interface{}) *Row {
	return tx.c.QueryRow(sql, args...)
}

func (tx *Tx) QueryRowEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) *Row {
	return tx.c.QueryRowEx(ctx, sql, options, args...)
}
