package pgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/internal/sanitize"
	"github.com/jackc/pgx/pgproto3"
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
	connPool   *ConnPool
	values     [][]byte
	fields     []FieldDescription
	rowCount   int
	columnIdx  int
	err        error
	startTime  time.Time
	sql        string
	args       []interface{}
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
			rows.conn.log(LogLevelInfo, "Query", map[string]interface{}{"sql": rows.sql, "args": logQueryArgs(rows.args), "time": endTime.Sub(rows.startTime), "rowCount": rows.rowCount})
		}
	} else if rows.conn.shouldLog(LogLevelError) {
		rows.conn.log(LogLevelError, "Query", map[string]interface{}{"sql": rows.sql, "args": logQueryArgs(rows.args)})
	}

	if rows.connPool != nil {
		rows.connPool.Release(rows.conn)
	}
}

func (rows *Rows) Err() error {
	return rows.err
}

// fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *Rows) fatal(err error) {
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

	for {
		msg, err := rows.conn.rxMsg()
		if err != nil {
			rows.fatal(err)
			return false
		}

		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			rows.fields = rows.conn.rxRowDescription(msg)
			for i := range rows.fields {
				if dt, ok := rows.conn.ConnInfo.DataTypeForOid(rows.fields[i].DataType); ok {
					rows.fields[i].DataTypeName = dt.Name
					rows.fields[i].FormatCode = TextFormatCode
				} else {
					rows.fatal(fmt.Errorf("unknown oid: %d", rows.fields[i].DataType))
					return false
				}
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != len(rows.fields) {
				rows.fatal(ProtocolError(fmt.Sprintf("Row description field count (%v) and data row field count (%v) do not match", len(rows.fields), len(msg.Values))))
				return false
			}

			rows.values = msg.Values
			return true
		case *pgproto3.CommandComplete:
			rows.Close()
			return false

		default:
			err = rows.conn.processContextFreeMsg(msg)
			if err != nil {
				rows.fatal(err)
				return false
			}
		}
	}
}

func (rows *Rows) nextColumn() ([]byte, *FieldDescription, bool) {
	if rows.closed {
		return nil, nil, false
	}
	if len(rows.fields) <= rows.columnIdx {
		rows.fatal(ProtocolError("No next column available"))
		return nil, nil, false
	}

	buf := rows.values[rows.columnIdx]
	fd := &rows.fields[rows.columnIdx]
	rows.columnIdx++
	return buf, fd, true
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
		rows.fatal(err)
		return err
	}

	for i, d := range dest {
		buf, fd, _ := rows.nextColumn()

		if d == nil {
			continue
		}

		if s, ok := d.(pgtype.BinaryDecoder); ok && fd.FormatCode == BinaryFormatCode {
			err = s.DecodeBinary(rows.conn.ConnInfo, buf)
			if err != nil {
				rows.fatal(scanArgError{col: i, err: err})
			}
		} else if s, ok := d.(pgtype.TextDecoder); ok && fd.FormatCode == TextFormatCode {
			err = s.DecodeText(rows.conn.ConnInfo, buf)
			if err != nil {
				rows.fatal(scanArgError{col: i, err: err})
			}
		} else {
			if dt, ok := rows.conn.ConnInfo.DataTypeForOid(fd.DataType); ok {
				value := dt.Value
				switch fd.FormatCode {
				case TextFormatCode:
					if textDecoder, ok := value.(pgtype.TextDecoder); ok {
						err = textDecoder.DecodeText(rows.conn.ConnInfo, buf)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else {
						rows.fatal(scanArgError{col: i, err: fmt.Errorf("%T is not a pgtype.TextDecoder", value)})
					}
				case BinaryFormatCode:
					if binaryDecoder, ok := value.(pgtype.BinaryDecoder); ok {
						err = binaryDecoder.DecodeBinary(rows.conn.ConnInfo, buf)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else {
						rows.fatal(scanArgError{col: i, err: fmt.Errorf("%T is not a pgtype.BinaryDecoder", value)})
					}
				default:
					rows.fatal(scanArgError{col: i, err: fmt.Errorf("unknown format code: %v", fd.FormatCode)})
				}

				if rows.Err() == nil {
					if scanner, ok := d.(sql.Scanner); ok {
						sqlSrc, err := pgtype.DatabaseSQLValue(rows.conn.ConnInfo, value)
						if err != nil {
							rows.fatal(err)
						}
						err = scanner.Scan(sqlSrc)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else if err := value.AssignTo(d); err != nil {
						rows.fatal(scanArgError{col: i, err: err})
					}
				}
			} else {
				rows.fatal(scanArgError{col: i, err: fmt.Errorf("unknown oid: %v", fd.DataType)})
			}
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
		buf, fd, _ := rows.nextColumn()

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.conn.ConnInfo.DataTypeForOid(fd.DataType); ok {
			value := dt.Value

			switch fd.FormatCode {
			case TextFormatCode:
				decoder := value.(pgtype.TextDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericText{}
				}
				err := decoder.DecodeText(rows.conn.ConnInfo, buf)
				if err != nil {
					rows.fatal(err)
				}
				values = append(values, decoder.(pgtype.Value).Get())
			case BinaryFormatCode:
				decoder := value.(pgtype.BinaryDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericBinary{}
				}
				err := decoder.DecodeBinary(rows.conn.ConnInfo, buf)
				if err != nil {
					rows.fatal(err)
				}
				values = append(values, value.Get())
			default:
				rows.fatal(errors.New("Unknown format code"))
			}
		} else {
			rows.fatal(errors.New("Unknown type"))
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}

	return values, rows.Err()
}

// Query executes sql with args. If there is an error the returned *Rows will
// be returned in an error state. So it is allowed to ignore the error returned
// from Query and handle it in *Rows.
func (c *Conn) Query(sql string, args ...interface{}) (*Rows, error) {
	return c.QueryEx(context.Background(), sql, nil, args...)
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

type QueryExOptions struct {
	ParameterOids  []pgtype.Oid
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
		rows.fatal(err)
		return rows, err
	}
	rows.unlockConn = true

	if options != nil && options.SimpleProtocol {
		err = c.initContext(ctx)
		if err != nil {
			rows.fatal(err)
			return rows, err
		}

		err = c.sanitizeAndSendSimpleQuery(sql, args...)
		if err != nil {
			rows.fatal(err)
			return rows, err
		}

		return rows, nil
	}

	ps, ok := c.preparedStatements[sql]
	if !ok {
		var err error
		ps, err = c.PrepareEx(ctx, "", sql, nil)
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}
	}
	rows.sql = ps.SQL
	rows.fields = ps.FieldDescriptions

	err = c.initContext(ctx)
	if err != nil {
		rows.fatal(err)
		return rows, rows.err
	}

	err = c.sendPreparedQuery(ps, args...)
	if err != nil {
		rows.fatal(err)
	}

	return rows, rows.err
}

func (c *Conn) sanitizeAndSendSimpleQuery(sql string, args ...interface{}) (err error) {
	if c.RuntimeParams["standard_conforming_strings"] != "on" {
		return errors.New("simple protocol queries must be run with standard_conforming_strings=on")
	}

	if c.RuntimeParams["client_encoding"] != "UTF8" {
		return errors.New("simple protocol queries must be run with client_encoding=UTF8")
	}

	valueArgs := make([]interface{}, len(args))
	for i, a := range args {
		valueArgs[i], err = convertSimpleArgument(c.ConnInfo, a)
		if err != nil {
			return err
		}
	}

	sql, err = sanitize.SanitizeSQL(sql, valueArgs...)
	if err != nil {
		return err
	}

	return c.sendSimpleQuery(sql)
}

func (c *Conn) QueryRowEx(ctx context.Context, sql string, options *QueryExOptions, args ...interface{}) *Row {
	rows, _ := c.QueryEx(ctx, sql, options, args...)
	return (*Row)(rows)
}
