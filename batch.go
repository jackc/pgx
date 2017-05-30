package pgx

import (
	"context"

	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
)

type batchItem struct {
	query             string
	arguments         []interface{}
	parameterOids     []pgtype.Oid
	resultFormatCodes []int16
}

type Batch struct {
	conn        *Conn
	items       []*batchItem
	resultsRead int
	sent        bool
}

// Begin starts a transaction with the default transaction mode for the
// current connection. To use a specific transaction mode see BeginEx.
func (c *Conn) BeginBatch() *Batch {
	// TODO - the type stuff below

	// err = c.waitForPreviousCancelQuery(ctx)
	// if err != nil {
	// 	return nil, err
	// }

	// if err := c.ensureConnectionReadyForQuery(); err != nil {
	// 	return nil, err
	// }

	// c.lastActivityTime = time.Now()

	// rows = c.getRows(sql, args)

	// if err := c.lock(); err != nil {
	// 	rows.fatal(err)
	// 	return rows, err
	// }
	// rows.unlockConn = true

	// err = c.initContext(ctx)
	// if err != nil {
	// 	rows.fatal(err)
	// 	return rows, rows.err
	// }

	// if options != nil && options.SimpleProtocol {
	// 	err = c.sanitizeAndSendSimpleQuery(sql, args...)
	// 	if err != nil {
	// 		rows.fatal(err)
	// 		return rows, err
	// 	}

	// 	return rows, nil
	// }

	return &Batch{conn: c}
}

func (b *Batch) Conn() *Conn {
	return b.conn
}

func (b *Batch) Queue(query string, arguments []interface{}, parameterOids []pgtype.Oid, resultFormatCodes []int16) {
	b.items = append(b.items, &batchItem{
		query:             query,
		arguments:         arguments,
		parameterOids:     parameterOids,
		resultFormatCodes: resultFormatCodes,
	})
}

func (b *Batch) Send(ctx context.Context, txOptions *TxOptions) error {
	buf := appendQuery(b.conn.wbuf, txOptions.beginSQL())

	for _, bi := range b.items {
		// TODO - don't parse if named prepared statement
		buf = appendParse(buf, "", bi.query, bi.parameterOids)

		var err error
		buf, err = appendBind(buf, "", "", b.conn.ConnInfo, bi.parameterOids, bi.arguments, bi.resultFormatCodes)
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

func (b *Batch) ExecResults() (CommandTag, error) {
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

func (b *Batch) QueryResults() (*Rows, error) {
	b.resultsRead++

	rows := b.conn.getRows("batch query", nil)

	fieldDescriptions, err := b.conn.readUntilRowDescription()
	if err != nil {
		rows.fatal(err)
		return nil, err
	}

	rows.fields = fieldDescriptions
	return rows, nil
}

func (b *Batch) QueryRowResults() *Row {
	rows, _ := b.QueryResults()
	return (*Row)(rows)

}

func (b *Batch) Finish() error {
	for i := b.resultsRead; i < len(b.items); i++ {
		_, err := b.ExecResults()
		if err != nil {
			return err
		}
	}

	// readyForQueryCount := 0

	// 	for {
	// 	msg, err := b.conn.rxMsg()
	// 	if err != nil {
	// 		return "", err
	// 	}

	// 	switch msg := msg.(type) {
	// case *pgproto3.ReadyForQuery:
	// 	c.rxReadyForQuery(msg)
	// 	default:
	// 		if err := b.conn.processContextFreeMsg(msg); err != nil {
	// 			return "", err
	// 		}
	// 	}
	// }

	// switch msg := msg.(type) {
	// case *pgproto3.ErrorResponse:
	// 	return c.rxErrorResponse(msg)
	// case *pgproto3.NotificationResponse:
	// 	c.rxNotificationResponse(msg)
	// case *pgproto3.ReadyForQuery:
	// 	c.rxReadyForQuery(msg)
	// case *pgproto3.ParameterStatus:
	// 	c.rxParameterStatus(msg)
	// }

	return nil
}
