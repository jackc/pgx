package pgx

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
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

func (rows *Rows) close() {
	if rows.closed {
		return
	}

	if rows.unlockConn {
		rows.conn.unlock()
		rows.unlockConn = false
	}

	rows.closed = true

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

func (rows *Rows) readUntilReadyForQuery() {
	for {
		t, r, err := rows.conn.rxMsg()
		if err != nil {
			rows.close()
			return
		}

		switch t {
		case readyForQuery:
			rows.conn.rxReadyForQuery(r)
			rows.close()
			return
		case rowDescription:
		case dataRow:
		case commandComplete:
		case bindComplete:
		case errorResponse:
			err = rows.conn.rxErrorResponse(r)
			if rows.err == nil {
				rows.err = err
			}
		default:
			err = rows.conn.processContextFreeMsg(t, r)
			if err != nil {
				rows.close()
				return
			}
		}
	}
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (rows *Rows) Close() {
	if rows.closed {
		return
	}
	rows.readUntilReadyForQuery()
	rows.close()
}

func (rows *Rows) Err() error {
	return rows.err
}

// abort signals that the query was not successfully sent to the server.
// This differs from Fatal in that it is not necessary to readUntilReadyForQuery
func (rows *Rows) abort(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.close()
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
		case readyForQuery:
			rows.conn.rxReadyForQuery(r)
			rows.close()
			return false
		case dataRow:
			fieldCount := r.readInt16()
			if int(fieldCount) != len(rows.fields) {
				rows.Fatal(ProtocolError(fmt.Sprintf("Row description field count (%v) and data row field count (%v) do not match", len(rows.fields), fieldCount)))
				return false
			}

			rows.mr = r
			return true
		case commandComplete:
		case bindComplete:
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

		// Check for []byte first as we allow sidestepping the decoding process and retrieving the raw bytes
		if b, ok := d.(*[]byte); ok {
			// If it actually is a bytea then pass it through decodeBytea (so it can be decoded if it is in text format)
			// Otherwise read the bytes directly regardless of what the actual type is.
			if vr.Type().DataType == ByteaOid {
				*b = decodeBytea(vr)
			} else {
				if vr.Len() != -1 {
					*b = vr.ReadBytes(vr.Len())
				} else {
					*b = nil
				}
			}
		} else if s, ok := d.(Scanner); ok {
			err = s.Scan(vr)
			if err != nil {
				rows.Fatal(scanArgError{col: i, err: err})
			}
		} else if s, ok := d.(sql.Scanner); ok {
			var val interface{}
			if 0 <= vr.Len() {
				switch vr.Type().DataType {
				case BoolOid:
					val = decodeBool(vr)
				case Int8Oid:
					val = int64(decodeInt8(vr))
				case Int2Oid:
					val = int64(decodeInt2(vr))
				case Int4Oid:
					val = int64(decodeInt4(vr))
				case TextOid, VarcharOid:
					val = decodeText(vr)
				case OidOid:
					val = int64(decodeOid(vr))
				case Float4Oid:
					val = float64(decodeFloat4(vr))
				case Float8Oid:
					val = decodeFloat8(vr)
				case DateOid:
					val = decodeDate(vr)
				case TimestampOid:
					val = decodeTimestamp(vr)
				case TimestampTzOid:
					val = decodeTimestampTz(vr)
				default:
					val = vr.ReadBytes(vr.Len())
				}
			}
			err = s.Scan(val)
			if err != nil {
				rows.Fatal(scanArgError{col: i, err: err})
			}
		} else if vr.Type().DataType == JsonOid || vr.Type().DataType == JsonbOid {
			// Because the argument passed to decodeJSON will escape the heap.
			// This allows d to be stack allocated and only copied to the heap when
			// we actually are decoding JSON. This saves one memory allocation per
			// row.
			d2 := d
			decodeJSON(vr, &d2)
		} else {
			if err := Decode(vr, d); err != nil {
				rows.Fatal(scanArgError{col: i, err: err})
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

		switch vr.Type().FormatCode {
		// All intrinsic types (except string) are encoded with binary
		// encoding so anything else should be treated as a string
		case TextFormatCode:
			values = append(values, vr.ReadString(vr.Len()))
		case BinaryFormatCode:
			switch vr.Type().DataType {
			case TextOid, VarcharOid:
				values = append(values, decodeText(vr))
			case BoolOid:
				values = append(values, decodeBool(vr))
			case ByteaOid:
				values = append(values, decodeBytea(vr))
			case Int8Oid:
				values = append(values, decodeInt8(vr))
			case Int2Oid:
				values = append(values, decodeInt2(vr))
			case Int4Oid:
				values = append(values, decodeInt4(vr))
			case OidOid:
				values = append(values, decodeOid(vr))
			case Float4Oid:
				values = append(values, decodeFloat4(vr))
			case Float8Oid:
				values = append(values, decodeFloat8(vr))
			case BoolArrayOid:
				values = append(values, decodeBoolArray(vr))
			case Int2ArrayOid:
				values = append(values, decodeInt2Array(vr))
			case Int4ArrayOid:
				values = append(values, decodeInt4Array(vr))
			case Int8ArrayOid:
				values = append(values, decodeInt8Array(vr))
			case Float4ArrayOid:
				values = append(values, decodeFloat4Array(vr))
			case Float8ArrayOid:
				values = append(values, decodeFloat8Array(vr))
			case TextArrayOid, VarcharArrayOid:
				values = append(values, decodeTextArray(vr))
			case TimestampArrayOid, TimestampTzArrayOid:
				values = append(values, decodeTimestampArray(vr))
			case DateOid:
				values = append(values, decodeDate(vr))
			case TimestampTzOid:
				values = append(values, decodeTimestampTz(vr))
			case TimestampOid:
				values = append(values, decodeTimestamp(vr))
			case InetOid, CidrOid:
				values = append(values, decodeInet(vr))
			case JsonOid:
				var d interface{}
				decodeJSON(vr, &d)
				values = append(values, d)
			case JsonbOid:
				var d interface{}
				decodeJSON(vr, &d)
				values = append(values, d)
			default:
				rows.Fatal(errors.New("Values cannot handle binary format non-intrinsic types"))
			}
		default:
			rows.Fatal(errors.New("Unknown format code"))
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
	c.lastActivityTime = time.Now()

	rows := c.getRows(sql, args)

	if err := c.lock(); err != nil {
		rows.abort(err)
		return rows, err
	}
	rows.unlockConn = true

	ps, ok := c.preparedStatements[sql]
	if !ok {
		var err error
		ps, err = c.Prepare("", sql)
		if err != nil {
			rows.abort(err)
			return rows, rows.err
		}
	}
	rows.sql = ps.SQL
	rows.fields = ps.FieldDescriptions
	err := c.sendPreparedQuery(ps, args...)
	if err != nil {
		rows.abort(err)
	}
	return rows, rows.err
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
