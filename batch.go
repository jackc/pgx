package pgx

import (
	"context"

	"github.com/jackc/pgconn"
	errors "golang.org/x/xerrors"
)

type batchItem struct {
	query     string
	arguments []interface{}
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips.
type Batch struct {
	items []*batchItem
}

// Queue queues a query to batch b. query can be an SQL query or the name of a prepared statement.
func (b *Batch) Queue(query string, arguments ...interface{}) {
	b.items = append(b.items, &batchItem{
		query:     query,
		arguments: arguments,
	})
}

type BatchResults interface {
	// Exec reads the results from the next query in the batch as if the query has been sent with Conn.Exec.
	Exec() (pgconn.CommandTag, error)

	// Query reads the results from the next query in the batch as if the query has been sent with Conn.Query.
	Query() (Rows, error)

	// QueryRow reads the results from the next query in the batch as if the query has been sent with Conn.QueryRow.
	QueryRow() Row

	// Close closes the batch operation. This must be called before the underlying connection can be used again. Any error
	// that occurred during a batch operation may have made it impossible to resyncronize the connection with the server.
	// In this case the underlying connection will have been closed.
	Close() error
}

type batchResults struct {
	ctx  context.Context
	conn *Conn
	mrr  *pgconn.MultiResultReader
	err  error
}

// Exec reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *batchResults) Exec() (pgconn.CommandTag, error) {
	if br.err != nil {
		return nil, br.err
	}

	if !br.mrr.NextResult() {
		err := br.mrr.Close()
		if err == nil {
			err = errors.New("no result")
		}
		return nil, err
	}

	return br.mrr.ResultReader().Close()
}

// Query reads the results from the next query in the batch as if the query has been sent with Query.
func (br *batchResults) Query() (Rows, error) {
	rows := br.conn.getRows(br.ctx, "batch query", nil)

	if br.err != nil {
		rows.err = br.err
		rows.closed = true
		return rows, br.err
	}

	if !br.mrr.NextResult() {
		rows.err = br.mrr.Close()
		if rows.err == nil {
			rows.err = errors.New("no result")
		}
		rows.closed = true
		return rows, rows.err
	}

	rows.resultReader = br.mrr.ResultReader()
	return rows, nil
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *batchResults) QueryRow() Row {
	rows, _ := br.Query()
	return (*connRow)(rows.(*connRows))

}

// Close closes the batch operation. Any error that occurred during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *batchResults) Close() error {
	if br.err != nil {
		return br.err
	}

	return br.mrr.Close()
}
