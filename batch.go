package pgx

import (
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	errors "golang.org/x/xerrors"
)

type batchItem struct {
	query             string
	arguments         []interface{}
	parameterOIDs     []pgtype.OID
	resultFormatCodes []int16
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips.
type Batch struct {
	items []*batchItem
}

// Queue queues a query to batch b. query can be an SQL query or the name of a prepared statement. parameterOIDs and
// resultFormatCodes should be nil if query is a prepared statement. Otherwise, parameterOIDs are required if there are
// parameters and resultFormatCodes are required if there is a result.
func (b *Batch) Queue(query string, arguments []interface{}, parameterOIDs []pgtype.OID, resultFormatCodes []int16) {
	b.items = append(b.items, &batchItem{
		query:             query,
		arguments:         arguments,
		parameterOIDs:     parameterOIDs,
		resultFormatCodes: resultFormatCodes,
	})
}

type BatchResults interface {
	// ExecResults reads the results from the next query in the batch as if the query has been sent with Exec.
	ExecResults() (pgconn.CommandTag, error)

	// QueryResults reads the results from the next query in the batch as if the query has been sent with Query.
	QueryResults() (Rows, error)

	// QueryRowResults reads the results from the next query in the batch as if the query has been sent with QueryRow.
	QueryRowResults() Row

	// Close closes the batch operation. Any error that occured during a batch operation may have made it impossible to
	// resyncronize the connection with the server. In this case the underlying connection will have been closed.
	Close() error
}

type batchResults struct {
	conn *Conn
	mrr  *pgconn.MultiResultReader
	err  error
}

// ExecResults reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *batchResults) ExecResults() (pgconn.CommandTag, error) {
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

// QueryResults reads the results from the next query in the batch as if the query has been sent with Query.
func (br *batchResults) QueryResults() (Rows, error) {
	rows := br.conn.getRows("batch query", nil)

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

// QueryRowResults reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *batchResults) QueryRowResults() Row {
	rows, _ := br.QueryResults()
	return (*connRow)(rows.(*connRows))

}

// Close closes the batch operation. Any error that occured during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *batchResults) Close() error {
	if br.err != nil {
		return br.err
	}

	return br.mrr.Close()
}
