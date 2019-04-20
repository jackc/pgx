package pgx

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
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
	conn                   *Conn
	items                  []*batchItem
	resultsRead            int
	pendingCommandComplete bool
	ctx                    context.Context
	err                    error
	inTx                   bool

	mrr *pgconn.MultiResultReader
}

// BeginBatch returns a *Batch query for c.
func (c *Conn) BeginBatch() *Batch {
	return &Batch{conn: c}
}

// BeginBatch returns a *Batch query for tx. Since this *Batch is already part
// of a transaction it will not automatically be wrapped in a transaction.
func (tx *Tx) BeginBatch() *Batch {
	return &Batch{conn: tx.conn, inTx: true}
}

// Conn returns the underlying connection that b will or was performed on.
func (b *Batch) Conn() *Conn {
	return b.conn
}

// Queue queues a query to batch b. parameterOIDs are required if there are
// parameters and query is not the name of a prepared statement.
// resultFormatCodes are required if there is a result.
func (b *Batch) Queue(query string, arguments []interface{}, parameterOIDs []pgtype.OID, resultFormatCodes []int16) {
	b.items = append(b.items, &batchItem{
		query:             query,
		arguments:         arguments,
		parameterOIDs:     parameterOIDs,
		resultFormatCodes: resultFormatCodes,
	})
}

// Send sends all queued queries to the server at once. All queries are run in an implicit transaction unless explicit
// transaction control statements are executed.
func (b *Batch) Send(ctx context.Context) error {
	if b.err != nil {
		return b.err
	}

	b.ctx = ctx

	batch := &pgconn.Batch{}

	for _, bi := range b.items {
		var parameterOIDs []pgtype.OID
		ps := b.conn.preparedStatements[bi.query]

		if ps != nil {
			parameterOIDs = ps.ParameterOIDs
		} else {
			parameterOIDs = bi.parameterOIDs
		}

		args, err := convertDriverValuers(bi.arguments)
		if err != nil {
			return err
		}

		paramFormats := make([]int16, len(args))
		paramValues := make([][]byte, len(args))
		for i := range args {
			paramFormats[i] = chooseParameterFormatCode(b.conn.ConnInfo, parameterOIDs[i], args[i])
			paramValues[i], err = newencodePreparedStatementArgument(b.conn.ConnInfo, parameterOIDs[i], args[i])
			if err != nil {
				return err
			}

		}

		if ps != nil {
			batch.ExecPrepared(ps.Name, paramValues, paramFormats, bi.resultFormatCodes)
		} else {
			oids := make([]uint32, len(parameterOIDs))
			for i := 0; i < len(parameterOIDs); i++ {
				oids[i] = uint32(parameterOIDs[i])
			}
			batch.ExecParams(bi.query, paramValues, oids, paramFormats, bi.resultFormatCodes)
		}
	}

	b.mrr = b.conn.pgConn.ExecBatch(ctx, batch)

	return nil
}

// ExecResults reads the results from the next query in the batch as if the
// query has been sent with Exec.
func (b *Batch) ExecResults() (pgconn.CommandTag, error) {
	if !b.mrr.NextResult() {
		err := b.mrr.Close()
		if err == nil {
			err = errors.New("no result")
		}
		return nil, err
	}

	return b.mrr.ResultReader().Close()
}

// QueryResults reads the results from the next query in the batch as if the
// query has been sent with Query.
func (b *Batch) QueryResults() (Rows, error) {
	rows := b.conn.getRows("batch query", nil)

	if !b.mrr.NextResult() {
		rows.err = b.mrr.Close()
		if rows.err == nil {
			rows.err = errors.New("no result")
		}
		rows.closed = true
		return rows, rows.err
	}

	rows.resultReader = b.mrr.ResultReader()
	return rows, nil
}

// QueryRowResults reads the results from the next query in the batch as if the
// query has been sent with QueryRow.
func (b *Batch) QueryRowResults() Row {
	rows, _ := b.QueryResults()
	return (*connRow)(rows.(*connRows))

}

// Close closes the batch operation. Any error that occured during a batch
// operation may have made it impossible to resyncronize the connection with the
// server. In this case the underlying connection will have been closed.
func (b *Batch) Close() (err error) {
	return b.mrr.Close()
}

func (b *Batch) die(err error) {
	if b.err != nil {
		return
	}

	b.err = err
	b.conn.die(err)
}
