package pgx

import (
	"context"
	"errors"
)

var (
	ErrNoSupportTxInBatchExecTx = errors.New("no support tx in batch exec transaction")
)

// BatchExec Executes queries using pgx.Batch without using an explicit transaction
// Example:
//
//	pgx.BatchExec(ctx, conn, func (b *pgx.Batch){
//		batcher.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
//		batcher.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")
//	})
//
// OR:
//
//	tx, err := conn.Begin(ctx)
//	if err != nil {
//		panic(err)
//	}
//
//	pgx.BatchExec(ctx, tx, func (b *pgx.Batch){
//		batcher.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
//		batcher.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")
//	})
//
//	tx.Commit() // or tx.Rollback(ctx)
func BatchExec(
	ctx context.Context,
	batcher interface {
		SendBatch(ctx context.Context, b *Batch) BatchResults
	},
	enrich func(b *Batch),
) error {
	b := &Batch{}

	enrich(b)

	results := batcher.SendBatch(ctx, b)
	defer results.Close()

	// Each query is checked for errors one after another.
	// Therefore, all queries in the loop are checked.
	// TODO: >= 1.23 use: `for range b.Len()`
	for i := 0; i < b.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}

	return results.Close()
}

// BatchExecTx Executes requests with pgx.Batch using an explicit transaction.
// The transaction is executed using `BEGIN` and `COMMIT` to use a single network request.
//
// CAUTION: Using pgx.Tx will not produce the expected execution result,
// nothing will happen when tx.Rollback() is requested.
// If pgx.Tx is passed to the function as a batcher, an ErrNoSupportTxInBatchExecTx error will be returned.
// Example:
//
//	pgx.BatchExecTx(ctx, conn, func (b *pgx.Batch){
//		batcher.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
//		batcher.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")
//	})
func BatchExecTx(
	ctx context.Context,
	batcher interface {
		SendBatch(ctx context.Context, b *Batch) BatchResults
	},
	enrich func(b *Batch),
) error {
	switch batcher.(type) {
	case Tx:
		return ErrNoSupportTxInBatchExecTx
	default:
		return BatchExec(ctx, batcher, func(b *Batch) {
			b.Queue("BEGIN;")
			enrich(b)
			b.Queue("COMMIT;")
		})
	}
}
