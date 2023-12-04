package pgx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// QueuedQuery is a query that has been queued for execution via a Batch.
type QueuedQuery struct {
	query     string
	arguments []any
	fn        batchItemFunc
	sd        *pgconn.StatementDescription
}

type batchItemFunc func(br BatchResults) error

// Query sets fn to be called when the response to qq is received.
func (qq *QueuedQuery) Query(fn func(rows Rows) error) {
	qq.fn = func(br BatchResults) error {
		rows, _ := br.Query()
		defer rows.Close()

		err := fn(rows)
		if err != nil {
			return err
		}
		rows.Close()

		return rows.Err()
	}
}

// Query sets fn to be called when the response to qq is received.
func (qq *QueuedQuery) QueryRow(fn func(row Row) error) {
	qq.fn = func(br BatchResults) error {
		row := br.QueryRow()
		return fn(row)
	}
}

// Exec sets fn to be called when the response to qq is received.
func (qq *QueuedQuery) Exec(fn func(ct pgconn.CommandTag) error) {
	qq.fn = func(br BatchResults) error {
		ct, err := br.Exec()
		if err != nil {
			return err
		}

		return fn(ct)
	}
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips. A Batch must only be sent once.
type Batch struct {
	queuedQueries []*QueuedQuery
}

// Queue queues a query to batch b. query can be an SQL query or the name of a prepared statement.
// The only pgx option argument that is supported is QueryRewriter. Queries are executed using the
// connection's DefaultQueryExecMode.
func (b *Batch) Queue(query string, arguments ...any) *QueuedQuery {
	qq := &QueuedQuery{
		query:     query,
		arguments: arguments,
	}
	b.queuedQueries = append(b.queuedQueries, qq)
	return qq
}

// Len returns number of queries that have been queued so far.
func (b *Batch) Len() int {
	return len(b.queuedQueries)
}

type BatchResults interface {
	// Exec reads the results from the next query in the batch as if the query has been sent with Conn.Exec. Prefer
	// calling Exec on the QueuedQuery.
	Exec() (pgconn.CommandTag, error)

	// Query reads the results from the next query in the batch as if the query has been sent with Conn.Query. Prefer
	// calling Query on the QueuedQuery.
	Query() (Rows, error)

	// QueryRow reads the results from the next query in the batch as if the query has been sent with Conn.QueryRow.
	// Prefer calling QueryRow on the QueuedQuery.
	QueryRow() Row

	// Close closes the batch operation. All unread results are read and any callback functions registered with
	// QueuedQuery.Query, QueuedQuery.QueryRow, or QueuedQuery.Exec will be called. If a callback function returns an
	// error or the batch encounters an error subsequent callback functions will not be called.
	//
	// Close must be called before the underlying connection can be used again. Any error that occurred during a batch
	// operation may have made it impossible to resyncronize the connection with the server. In this case the underlying
	// connection will have been closed.
	//
	// Close is safe to call multiple times. If it returns an error subsequent calls will return the same error. Callback
	// functions will not be rerun.
	Close() error
}

type batchResults struct {
	ctx       context.Context
	conn      *Conn
	mrr       *pgconn.MultiResultReader
	err       error
	b         *Batch
	qqIdx     int
	closed    bool
	endTraced bool
}

// Exec reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *batchResults) Exec() (pgconn.CommandTag, error) {
	if br.err != nil {
		return pgconn.CommandTag{}, br.err
	}
	if br.closed {
		return pgconn.CommandTag{}, fmt.Errorf("batch already closed")
	}

	query, arguments, _ := br.nextQueryAndArgs()

	if !br.mrr.NextResult() {
		err := br.mrr.Close()
		if err == nil {
			err = errors.New("no result")
		}
		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:  query,
				Args: arguments,
				Err:  err,
			})
		}
		return pgconn.CommandTag{}, err
	}

	commandTag, err := br.mrr.ResultReader().Close()
	if err != nil {
		br.err = err
		br.mrr.Close()
	}

	if br.conn.batchTracer != nil {
		br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
			SQL:        query,
			Args:       arguments,
			CommandTag: commandTag,
			Err:        br.err,
		})
	}

	return commandTag, br.err
}

// Query reads the results from the next query in the batch as if the query has been sent with Query.
func (br *batchResults) Query() (Rows, error) {
	query, arguments, ok := br.nextQueryAndArgs()
	if !ok {
		query = "batch query"
	}

	if br.err != nil {
		return &baseRows{err: br.err, closed: true}, br.err
	}

	if br.closed {
		alreadyClosedErr := fmt.Errorf("batch already closed")
		return &baseRows{err: alreadyClosedErr, closed: true}, alreadyClosedErr
	}

	rows := br.conn.getRows(br.ctx, query, arguments)
	rows.batchTracer = br.conn.batchTracer

	if !br.mrr.NextResult() {
		rows.err = br.mrr.Close()
		if rows.err == nil {
			rows.err = errors.New("no result")
		}
		rows.closed = true

		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:  query,
				Args: arguments,
				Err:  rows.err,
			})
		}

		return rows, rows.err
	}

	rows.resultReader = br.mrr.ResultReader()
	return rows, nil
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *batchResults) QueryRow() Row {
	rows, _ := br.Query()
	return (*connRow)(rows.(*baseRows))

}

// Close closes the batch operation. Any error that occurred during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *batchResults) Close() error {
	defer func() {
		if !br.endTraced {
			if br.conn != nil && br.conn.batchTracer != nil {
				br.conn.batchTracer.TraceBatchEnd(br.ctx, br.conn, TraceBatchEndData{Err: br.err})
			}
			br.endTraced = true
		}
	}()

	if br.err != nil {
		return br.err
	}

	if br.closed {
		return nil
	}

	// Read and run fn for all remaining items
	for br.err == nil && !br.closed && br.b != nil && br.qqIdx < len(br.b.queuedQueries) {
		if br.b.queuedQueries[br.qqIdx].fn != nil {
			err := br.b.queuedQueries[br.qqIdx].fn(br)
			if err != nil {
				br.err = err
			}
		} else {
			br.Exec()
		}
	}

	br.closed = true

	err := br.mrr.Close()
	if br.err == nil {
		br.err = err
	}

	return br.err
}

func (br *batchResults) earlyError() error {
	return br.err
}

func (br *batchResults) nextQueryAndArgs() (query string, args []any, ok bool) {
	if br.b != nil && br.qqIdx < len(br.b.queuedQueries) {
		bi := br.b.queuedQueries[br.qqIdx]
		query = bi.query
		args = bi.arguments
		ok = true
		br.qqIdx++
	}
	return
}

type pipelineBatchResults struct {
	ctx       context.Context
	conn      *Conn
	pipeline  *pgconn.Pipeline
	lastRows  *baseRows
	err       error
	b         *Batch
	qqIdx     int
	closed    bool
	endTraced bool
}

// Exec reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *pipelineBatchResults) Exec() (pgconn.CommandTag, error) {
	if br.err != nil {
		return pgconn.CommandTag{}, br.err
	}
	if br.closed {
		return pgconn.CommandTag{}, fmt.Errorf("batch already closed")
	}
	if br.lastRows != nil && br.lastRows.err != nil {
		return pgconn.CommandTag{}, br.err
	}

	query, arguments, _ := br.nextQueryAndArgs()

	results, err := br.pipeline.GetResults()
	if err != nil {
		br.err = err
		return pgconn.CommandTag{}, br.err
	}
	var commandTag pgconn.CommandTag
	switch results := results.(type) {
	case *pgconn.ResultReader:
		commandTag, br.err = results.Close()
	default:
		return pgconn.CommandTag{}, fmt.Errorf("unexpected pipeline result: %T", results)
	}

	if br.conn.batchTracer != nil {
		br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
			SQL:        query,
			Args:       arguments,
			CommandTag: commandTag,
			Err:        br.err,
		})
	}

	return commandTag, br.err
}

// Query reads the results from the next query in the batch as if the query has been sent with Query.
func (br *pipelineBatchResults) Query() (Rows, error) {
	if br.err != nil {
		return &baseRows{err: br.err, closed: true}, br.err
	}

	if br.closed {
		alreadyClosedErr := fmt.Errorf("batch already closed")
		return &baseRows{err: alreadyClosedErr, closed: true}, alreadyClosedErr
	}

	if br.lastRows != nil && br.lastRows.err != nil {
		br.err = br.lastRows.err
		return &baseRows{err: br.err, closed: true}, br.err
	}

	query, arguments, ok := br.nextQueryAndArgs()
	if !ok {
		query = "batch query"
	}

	rows := br.conn.getRows(br.ctx, query, arguments)
	rows.batchTracer = br.conn.batchTracer
	br.lastRows = rows

	results, err := br.pipeline.GetResults()
	if err != nil {
		br.err = err
		rows.err = err
		rows.closed = true

		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:  query,
				Args: arguments,
				Err:  err,
			})
		}
	} else {
		switch results := results.(type) {
		case *pgconn.ResultReader:
			rows.resultReader = results
		default:
			err = fmt.Errorf("unexpected pipeline result: %T", results)
			br.err = err
			rows.err = err
			rows.closed = true
		}
	}

	return rows, rows.err
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *pipelineBatchResults) QueryRow() Row {
	rows, _ := br.Query()
	return (*connRow)(rows.(*baseRows))

}

// Close closes the batch operation. Any error that occurred during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *pipelineBatchResults) Close() error {
	defer func() {
		if !br.endTraced {
			if br.conn.batchTracer != nil {
				br.conn.batchTracer.TraceBatchEnd(br.ctx, br.conn, TraceBatchEndData{Err: br.err})
			}
			br.endTraced = true
		}
	}()

	if br.err == nil && br.lastRows != nil && br.lastRows.err != nil {
		br.err = br.lastRows.err
		return br.err
	}

	if br.closed {
		return br.err
	}

	// Read and run fn for all remaining items
	for br.err == nil && !br.closed && br.b != nil && br.qqIdx < len(br.b.queuedQueries) {
		if br.b.queuedQueries[br.qqIdx].fn != nil {
			err := br.b.queuedQueries[br.qqIdx].fn(br)
			if err != nil {
				br.err = err
			}
		} else {
			br.Exec()
		}
	}

	br.closed = true

	err := br.pipeline.Close()
	if br.err == nil {
		br.err = err
	}

	return br.err
}

func (br *pipelineBatchResults) earlyError() error {
	return br.err
}

func (br *pipelineBatchResults) nextQueryAndArgs() (query string, args []any, ok bool) {
	if br.b != nil && br.qqIdx < len(br.b.queuedQueries) {
		bi := br.b.queuedQueries[br.qqIdx]
		query = bi.query
		args = bi.arguments
		ok = true
		br.qqIdx++
	}
	return
}
