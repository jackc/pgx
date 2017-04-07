package pgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/pgtype"
)

// Row is a convenience wrapper over Rows that is returned by QueryRow.
type Row Rows

// Scan works the same as (*Rows Scan) with the following exceptions. If no
// rows were found it returns ErrNoRows. If multiple rows are returned it
// ignores all but the first.
func (r *Row) Scan(dest ...interface{}) (err error) {
	rows := (*Rows)(r)

	if rows.Err() != nil {
		return rows.Err()
	}

	if !rows.Next() {
		if rows.Err() == nil {
			return ErrNoRows
		}
		return rows.Err()
	}

	rows.Scan(dest...)
	rows.Close()
	return rows.Err()
}

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
type Rows struct {
	conn       *Conn
	mr         *msgReader
	fields     []FieldDescription
	vr         ValueReader
	rowCount   int
	columnIdx  int
	err        error
	startTime  time.Time
	sql        string
	args       []interface{}
	afterClose func(*Rows)
	unlockConn bool
	closed     bool
}

func (rows *Rows) FieldDescriptions() []FieldDescription {
	return rows.fields
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (rows *Rows) Close() {
	if rows.closed {
		return
	}

	if rows.unlockConn {
		rows.conn.unlock()
		rows.unlockConn = false
	}

	rows.closed = true

	rows.err = rows.conn.termContext(rows.err)

	if rows.err == nil {
		if rows.conn.shouldLog(LogLevelInfo) {
			endTime := time.Now()
			rows.conn.log(LogLevelInfo, "Query", "sql", rows.sql, "args", logQueryArgs(rows.args), "time", endTime.Sub(rows.startTime), "rowCount", rows.rowCount)
		}
	} else if rows.conn.shouldLog(LogLevelError) {
		rows.conn.log(LogLevelError, "Query", "sql", rows.sql, "args", logQueryArgs(rows.args))
	}

	if rows.afterClose != nil {
		rows.afterClose(rows)
	}
}

func (rows *Rows) Err() error {
	return rows.err
}

// Fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *Rows) Fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

// Next prepares the next row for reading. It returns true if there is another
// row and false if no more rows are available. It automatically closes rows
// when all rows are read.
func (rows *Rows) Next() bool {
	if rows.closed {
		return false
	}

	rows.rowCount++
	rows.columnIdx = 0
	rows.vr = ValueReader{}

	for {
		t, r, err := rows.conn.rxMsg()
		if err != nil {
			rows.Fatal(err)
			return false
		}

		switch t {
		case dataRow:
			fieldCount := r.readInt16()
			if int(fieldCount) != len(rows.fields) {
				rows.Fatal(ProtocolError(fmt.Sprintf("Row description field count (%v) and data row field count (%v) do not match", len(rows.fields), fieldCount)))
				return false
			}

			rows.mr = r
			return true
		case commandComplete:
			rows.Close()
			return false

		default:
			err = rows.conn.processContextFreeMsg(t, r)
			if err != nil {
				rows.Fatal(err)
				return false
			}
		}
	}
}

// Conn returns the *Conn this *Rows is using.
func (rows *Rows) Conn() *Conn {
	return rows.conn
}

func (rows *Rows) nextColumn() (*ValueReader, bool) {
	if rows.closed {
		return nil, false
	}
	if len(rows.fields) <= rows.columnIdx {
		rows.Fatal(ProtocolError("No next column available"))
		return nil, false
	}

	if rows.vr.Len() > 0 {
		rows.mr.readBytes(rows.vr.Len())
	}

	fd := &rows.fields[rows.columnIdx]
	rows.columnIdx++
	size := rows.mr.readInt32()
	rows.vr = ValueReader{mr: rows.mr, fd: fd, valueBytesRemaining: size}
	return &rows.vr, true
}

type scanArgError struct {
	col int
	err error
}

func (e scanArgError) Error() string {
	return fmt.Sprintf("can't scan into dest[%d]: %v", e.col, e.err)
}

// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types, values implementing the Scanner
// interface, []byte, and nil. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
func (rows *Rows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = fmt.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.fields))
		rows.Fatal(err)
		return err
	}

	for i, d := range dest {
		vr, _ := rows.nextColumn()

		if d == nil {
			continue
		}

		if s, ok := d.(pgtype.BinaryDecoder); ok && vr.Type().FormatCode == BinaryFormatCode {
			err = s.DecodeBinary(rows.conn.ConnInfo, vr.bytes())
			if err != nil {
				rows.Fatal(scanArgError{col: i, err: err})
			}
		} else if s, ok := d.(pgtype.TextDecoder); ok && vr.Type().FormatCode == TextFormatCode {
			err = s.DecodeText(rows.conn.ConnInfo, vr.bytes())
			if err != nil {
				rows.Fatal(scanArgError{col: i, err: err})
			}
		} else {
			if dt, ok := rows.conn.ConnInfo.DataTypeForOid(vr.Type().DataType); ok {
				value := dt.Value
				switch vr.Type().FormatCode {
				case TextFormatCode:
					if textDecoder, ok := value.(pgtype.TextDecoder); ok {
						err = textDecoder.DecodeText(rows.conn.ConnInfo, vr.bytes())
						if err != nil {
							vr.Fatal(err)
						}
					} else {
						vr.Fatal(fmt.Errorf("%T is not a pgtype.TextDecoder", value))
					}
				case BinaryFormatCode:
					if binaryDecoder, ok := value.(pgtype.BinaryDecoder); ok {
						err = binaryDecoder.DecodeBinary(rows.conn.ConnInfo, vr.bytes())
						if err != nil {
							vr.Fatal(err)
						}
					} else {
						vr.Fatal(fmt.Errorf("%T is not a pgtype.BinaryDecoder", value))
					}
				default:
					vr.Fatal(fmt.Errorf("unknown format code: %v", vr.Type().FormatCode))
				}

				if vr.Err() == nil {
					if scanner, ok := d.(sql.Scanner); ok {
						sqlSrc, err := pgtype.DatabaseSQLValue(rows.conn.ConnInfo, value)
						if err != nil {
							rows.Fatal(err)
						}
						err = scanner.Scan(sqlSrc)
						if err != nil {
							rows.Fatal(scanArgError{col: i, err: err})
						}
					} else if err := value.AssignTo(d); err != nil {
						vr.Fatal(err)
					}
				}
			} else {
				rows.Fatal(scanArgError{col: i, err: fmt.Errorf("unknown oid: %v", vr.Type().DataType)})
			}
		}
		if vr.Err() != nil {
			rows.Fatal(scanArgError{col: i, err: vr.Err()})
		}

		if rows.Err() != nil {
			return rows.Err()
		}
	}

	return nil
}

// Values returns an array of the row values
func (rows *Rows) Values() ([]interface{}, error) {
	if rows.closed {
		return nil, errors.New("rows is closed")
	}

	values := make([]interface{}, 0, len(rows.fields))

	for range rows.fields {
		vr, _ := rows.nextColumn()

		if vr.Len() == -1 {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.conn.ConnInfo.DataTypeForOid(vr.Type().DataType); ok {
			value := dt.Value

			switch vr.Type().FormatCode {
			case TextFormatCode:
				decoder := value.(pgtype.TextDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericText{}
				}
				err := decoder.DecodeText(rows.conn.ConnInfo, vr.bytes())
				if err != nil {
					rows.Fatal(err)
				}
				values = append(values, decoder.(pgtype.Value).Get())
			case BinaryFormatCode:
				decoder := value.(pgtype.BinaryDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericBinary{}
				}
				err := decoder.DecodeBinary(rows.conn.ConnInfo, vr.bytes())
				if err != nil {
					rows.Fatal(err)
				}
				values = append(values, value.Get())
			default:
				rows.Fatal(errors.New("Unknown format code"))
			}
		} else {
			rows.Fatal(errors.New("Unknown type"))
		}

		if vr.Err() != nil {
			rows.Fatal(vr.Err())
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}

	return values, rows.Err()
}

// AfterClose adds f to a LILO queue of functions that will be called when
// rows is closed.
func (rows *Rows) AfterClose(f func(*Rows)) {
	if rows.afterClose == nil {
		rows.afterClose = f
	} else {
		prevFn := rows.afterClose
		rows.afterClose = func(rows *Rows) {
			f(rows)
			prevFn(rows)
		}
	}
}

// Query executes sql with args. If there is an error the returned *Rows will
// be returned in an error state. So it is allowed to ignore the error returned
// from Query and handle it in *Rows.
func (c *Conn) Query(sql string, args ...interface{}) (*Rows, error) {
	return c.QueryContext(context.Background(), sql, args...)
}

func (c *Conn) getRows(sql string, args []interface{}) *Rows {
	if len(c.preallocatedRows) == 0 {
		c.preallocatedRows = make([]Rows, 64)
	}

	r := &c.preallocatedRows[len(c.preallocatedRows)-1]
	c.preallocatedRows = c.preallocatedRows[0 : len(c.preallocatedRows)-1]

	r.conn = c
	r.startTime = c.lastActivityTime
	r.sql = sql
	r.args = args

	return r
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned *Row. That *Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := c.Query(sql, args...)
	return (*Row)(rows)
}

func (c *Conn) QueryContext(ctx context.Context, sql string, args ...interface{}) (rows *Rows, err error) {
	err = c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return nil, err
	}

	c.lastActivityTime = time.Now()

	rows = c.getRows(sql, args)

	if err := c.lock(); err != nil {
		rows.Fatal(err)
		return rows, err
	}
	rows.unlockConn = true

	ps, ok := c.preparedStatements[sql]
	if !ok {
		var err error
		ps, err = c.PrepareExContext(ctx, "", sql, nil)
		if err != nil {
			rows.Fatal(err)
			return rows, rows.err
		}
	}
	rows.sql = ps.SQL
	rows.fields = ps.FieldDescriptions

	err = c.initContext(ctx)
	if err != nil {
		rows.Fatal(err)
		return rows, err
	}

	err = c.sendPreparedQuery(ps, args...)
	if err != nil {
		rows.Fatal(err)
		err = c.termContext(err)
	}

	return rows, err
}

type QueryExOptions struct {
	SimpleProtocol bool
}

func (c *Conn) QueryEx(ctx context.Context, sql string, options *QueryExOptions, args ...interface{}) (rows *Rows, err error) {
	err = c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return nil, err
	}

	c.lastActivityTime = time.Now()

	rows = c.getRows(sql, args)

	if err := c.lock(); err != nil {
		rows.Fatal(err)
		return rows, err
	}
	rows.unlockConn = true

	if options.SimpleProtocol {
		c.sendSimpleQuery(sql, args...)
	}

	ps, ok := c.preparedStatements[sql]
	if !ok {
		var err error
		ps, err = c.PrepareExContext(ctx, "", sql, nil)
		if err != nil {
			rows.Fatal(err)
			return rows, rows.err
		}
	}
	rows.sql = ps.SQL
	rows.fields = ps.FieldDescriptions

	err = c.initContext(ctx)
	if err != nil {
		rows.Fatal(err)
		return rows, err
	}

	err = c.sendPreparedQuery(ps, args...)
	if err != nil {
		rows.Fatal(err)
		err = c.termContext(err)
	}

	return rows, err
}

func (c *Conn) QueryRowContext(ctx context.Context, sql string, args ...interface{}) *Row {
	rows, _ := c.QueryContext(ctx, sql, args...)
	return (*Row)(rows)
}

func (c *Conn) QueryRowEx(ctx context.Context, sql string, options *QueryExOptions, args ...interface{}) *Row {
	rows, _ := c.QueryEx(ctx, sql, options, args...)
	return (*Row)(rows)
}
