package pgx

import (
	"bytes"
	"fmt"
)

// CopyToRows returns a CopyToSource interface over the provided rows slice
// making it usable by *Conn.CopyTo.
func CopyToRows(rows [][]interface{}) CopyToSource {
	return &copyToRows{rows: rows, idx: -1}
}

type copyToRows struct {
	rows [][]interface{}
	idx  int
}

func (ctr *copyToRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyToRows) Values() ([]interface{}, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyToRows) Err() error {
	return nil
}

// CopyToSource is the interface used by *Conn.CopyTo as the source for copy data.
type CopyToSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). When there are no more rows available or an error
	// has occurred it returns false.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]interface{}, error)

	// Err returns any error that has been encountered by the CopyToSource. If
	// this is not nil *Conn.CopyTo will abort the copy.
	Err() error
}

type copyTo struct {
	conn          *Conn
	tableName     string
	columnNames   []string
	rowSrc        CopyToSource
	readerErrChan chan error
}

func (ct *copyTo) readUntilReadyForQuery() {
	for {
		t, r, err := ct.conn.rxMsg()
		if err != nil {
			ct.readerErrChan <- err
			close(ct.readerErrChan)
			return
		}

		switch t {
		case readyForQuery:
			ct.conn.rxReadyForQuery(r)
			close(ct.readerErrChan)
			return
		case commandComplete:
		case errorResponse:
			ct.readerErrChan <- ct.conn.rxErrorResponse(r)
		default:
			err = ct.conn.processContextFreeMsg(t, r)
			if err != nil {
				ct.readerErrChan <- ct.conn.processContextFreeMsg(t, r)
			}
		}
	}
}

func (ct *copyTo) waitForReaderDone() error {
	var err error
	for err = range ct.readerErrChan {
	}
	return err
}

func (ct *copyTo) run() (int, error) {
	quotedTableName := quoteIdentifier(ct.tableName)
	buf := &bytes.Buffer{}
	for i, cn := range ct.columnNames {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := buf.String()

	ps, err := ct.conn.Prepare("", fmt.Sprintf("select %s from %s", quotedColumnNames, quotedTableName))
	if err != nil {
		return 0, err
	}

	err = ct.conn.sendSimpleQuery(fmt.Sprintf("copy %s ( %s ) from stdin binary;", quotedTableName, quotedColumnNames))
	if err != nil {
		return 0, err
	}

	err = ct.conn.readUntilCopyInResponse()
	if err != nil {
		return 0, err
	}

	go ct.readUntilReadyForQuery()
	defer ct.waitForReaderDone()

	wbuf := newWriteBuf(ct.conn, copyData)

	wbuf.WriteBytes([]byte("PGCOPY\n\377\r\n\000"))
	wbuf.WriteInt32(0)
	wbuf.WriteInt32(0)

	var sentCount int

	for ct.rowSrc.Next() {
		select {
		case err = <-ct.readerErrChan:
			return 0, err
		default:
		}

		if len(wbuf.buf) > 65536 {
			wbuf.closeMsg()
			_, err = ct.conn.conn.Write(wbuf.buf)
			if err != nil {
				ct.conn.die(err)
				return 0, err
			}

			// Directly manipulate wbuf to reset to reuse the same buffer
			wbuf.buf = wbuf.buf[0:5]
			wbuf.buf[0] = copyData
			wbuf.sizeIdx = 1
		}

		sentCount++

		values, err := ct.rowSrc.Values()
		if err != nil {
			ct.cancelCopyIn()
			return 0, err
		}
		if len(values) != len(ct.columnNames) {
			ct.cancelCopyIn()
			return 0, fmt.Errorf("expected %d values, got %d values", len(ct.columnNames), len(values))
		}

		wbuf.WriteInt16(int16(len(ct.columnNames)))
		for i, val := range values {
			err = Encode(wbuf, ps.FieldDescriptions[i].DataType, val)
			if err != nil {
				ct.cancelCopyIn()
				return 0, err
			}

		}
	}

	if ct.rowSrc.Err() != nil {
		ct.cancelCopyIn()
		return 0, ct.rowSrc.Err()
	}

	wbuf.WriteInt16(-1) // terminate the copy stream

	wbuf.startMsg(copyDone)
	wbuf.closeMsg()
	_, err = ct.conn.conn.Write(wbuf.buf)
	if err != nil {
		ct.conn.die(err)
		return 0, err
	}

	err = ct.waitForReaderDone()
	if err != nil {
		return 0, err
	}
	return sentCount, nil
}

func (c *Conn) readUntilCopyInResponse() error {
	for {
		var t byte
		var r *msgReader
		t, r, err := c.rxMsg()
		if err != nil {
			return err
		}

		switch t {
		case copyInResponse:
			return nil
		default:
			err = c.processContextFreeMsg(t, r)
			if err != nil {
				return err
			}
		}
	}
}

func (ct *copyTo) cancelCopyIn() error {
	wbuf := newWriteBuf(ct.conn, copyFail)
	wbuf.WriteCString("client error: abort")
	wbuf.closeMsg()
	_, err := ct.conn.conn.Write(wbuf.buf)
	if err != nil {
		ct.conn.die(err)
		return err
	}

	return nil
}

// CopyTo uses the PostgreSQL copy protocol to perform bulk data insertion.
// It returns the number of rows copied and an error.
//
// CopyTo requires all values use the binary format. Almost all types
// implemented by pgx use the binary format by default. Types implementing
// Encoder can only be used if they encode to the binary format.
func (c *Conn) CopyTo(tableName string, columnNames []string, rowSrc CopyToSource) (int, error) {
	ct := &copyTo{
		conn:          c,
		tableName:     tableName,
		columnNames:   columnNames,
		rowSrc:        rowSrc,
		readerErrChan: make(chan error),
	}

	return ct.run()
}
