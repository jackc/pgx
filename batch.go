package pgx

import (
	"context"

	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
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
	conn        *Conn
	connPool    *ConnPool
	items       []*batchItem
	resultsRead int
	sent        bool
	ctx         context.Context
	err         error
}

// BeginBatch returns a *Batch query for c.
func (c *Conn) BeginBatch() *Batch {
	return &Batch{conn: c}
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

// Send sends all queued queries to the server at once. All queries are wrapped
// in a transaction. The transaction can optionally be configured with
// txOptions. The context is in effect until the Batch is closed.
func (b *Batch) Send(ctx context.Context, txOptions *TxOptions) error {
	if b.err != nil {
		return b.err
	}

	b.ctx = ctx

	err := b.conn.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return err
	}

	if err := b.conn.ensureConnectionReadyForQuery(); err != nil {
		return err
	}

	err = b.conn.initContext(ctx)
	if err != nil {
		return err
	}

	buf := appendQuery(b.conn.wbuf, txOptions.beginSQL())

	for _, bi := range b.items {
		var psName string
		var psParameterOIDs []pgtype.OID

		if ps, ok := b.conn.preparedStatements[bi.query]; ok {
			psName = ps.Name
			psParameterOIDs = ps.ParameterOIDs
		} else {
			psParameterOIDs = bi.parameterOIDs
			buf = appendParse(buf, "", bi.query, psParameterOIDs)
		}

		var err error
		buf, err = appendBind(buf, "", psName, b.conn.ConnInfo, psParameterOIDs, bi.arguments, bi.resultFormatCodes)
		if err != nil {
			return err
		}

		buf = appendDescribe(buf, 'P', "")
		buf = appendExecute(buf, "", 0)
	}

	buf = appendSync(buf)
	buf = appendQuery(buf, "commit")

	n, err := b.conn.conn.Write(buf)
	if err != nil {
		if fatalWriteErr(n, err) {
			b.conn.die(err)
		}
		return err
	}

	// expect ReadyForQuery from sync and from commit
	b.conn.pendingReadyForQueryCount = b.conn.pendingReadyForQueryCount + 2

	b.sent = true

	for {
		msg, err := b.conn.rxMsg()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return nil
		default:
			if err := b.conn.processContextFreeMsg(msg); err != nil {
				return err
			}
		}
	}

	return nil
}

// ExecResults reads the results from the next query in the batch as if the
// query has been sent with Exec.
func (b *Batch) ExecResults() (CommandTag, error) {
	if b.err != nil {
		return "", b.err
	}

	select {
	case <-b.ctx.Done():
		b.die(b.ctx.Err())
		return "", b.ctx.Err()
	default:
	}

	b.resultsRead++

	for {
		msg, err := b.conn.rxMsg()
		if err != nil {
			return "", err
		}

		switch msg := msg.(type) {
		case *pgproto3.CommandComplete:
			return CommandTag(msg.CommandTag), nil
		default:
			if err := b.conn.processContextFreeMsg(msg); err != nil {
				return "", err
			}
		}
	}
}

// QueryResults reads the results from the next query in the batch as if the
// query has been sent with Query.
func (b *Batch) QueryResults() (*Rows, error) {
	rows := b.conn.getRows("batch query", nil)

	if b.err != nil {
		rows.fatal(b.err)
		return rows, b.err
	}

	select {
	case <-b.ctx.Done():
		b.die(b.ctx.Err())
		rows.fatal(b.err)
		return rows, b.ctx.Err()
	default:
	}

	b.resultsRead++

	fieldDescriptions, err := b.conn.readUntilRowDescription()
	if err != nil {
		b.die(err)
		rows.fatal(b.err)
		return rows, err
	}

	rows.batch = b
	rows.fields = fieldDescriptions
	return rows, nil
}

// QueryRowResults reads the results from the next query in the batch as if the
// query has been sent with QueryRow.
func (b *Batch) QueryRowResults() *Row {
	rows, _ := b.QueryResults()
	return (*Row)(rows)

}

// Close closes the batch operation. Any error that occured during a batch
// operation may have made it impossible to resyncronize the connection with the
// server. In this case the underlying connection will have been closed.
func (b *Batch) Close() (err error) {
	if b.err != nil {
		return b.err
	}

	defer func() {
		err = b.conn.termContext(err)
		if b.conn != nil && b.connPool != nil {
			b.connPool.Release(b.conn)
		}
	}()

	for i := b.resultsRead; i < len(b.items); i++ {
		if _, err = b.ExecResults(); err != nil {
			return err
		}
	}

	if err = b.conn.ensureConnectionReadyForQuery(); err != nil {
		return err
	}

	return nil
}

func (b *Batch) die(err error) {
	if b.err != nil {
		return
	}

	b.err = err
	b.conn.die(err)

	if b.conn != nil && b.connPool != nil {
		b.connPool.Release(b.conn)
	}
}
