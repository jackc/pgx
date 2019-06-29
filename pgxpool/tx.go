package pgxpool

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type Tx struct {
	t *pgx.Tx
	c *Conn
}

func (tx *Tx) Commit(ctx context.Context) error {
	err := tx.t.Commit(ctx)
	if tx.c != nil {
		tx.c.Release()
		tx.c = nil
	}
	return err
}

func (tx *Tx) Rollback(ctx context.Context) error {
	err := tx.t.Rollback(ctx)
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

func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return tx.c.Query(ctx, sql, args...)
}

func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return tx.c.QueryRow(ctx, sql, args...)
}

func (tx *Tx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return tx.c.SendBatch(ctx, b)
}

func (tx *Tx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return tx.c.CopyFrom(ctx, tableName, columnNames, rowSrc)
}
