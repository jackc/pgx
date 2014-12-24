package pgx

import (
	"errors"
	"fmt"
	"time"
)

// Row is a convenience wrapper over Rows that is returned by QueryRow.
type Row Rows

// Scan reads the values from the row into dest values positionally. dest can
// include pointers to core types and the Scanner interface. If no rows were
// found it returns ErrNoRows. If multiple rows are returned it ignores all but
// the first.
func (r *Row) Scan(dest ...interface{}) (err error) {
	rows := (*Rows)(r)

	if rows.Err() != nil {
		return rows.Err()
	}

	if !rows.Next() {
		if rows.Err() == nil {
			return ErrNoRows
		} else {
			return rows.Err()
		}
	}

	rows.Scan(dest...)
	rows.Close()
	return rows.Err()
}

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
type Rows struct {
	pool      *ConnPool
	conn      *Conn
	mr        *msgReader
	fields    []FieldDescription
	vr        ValueReader
	rowCount  int
	columnIdx int
	err       error
	closed    bool
	startTime time.Time
	sql       string
	args      []interface{}
	logger    Logger
}

func (rows *Rows) FieldDescriptions() []FieldDescription {
	return rows.fields
}

func (rows *Rows) close() {
	if rows.closed {
		return
	}

	if rows.pool != nil {
		rows.pool.Release(rows.conn)
		rows.pool = nil
	}

	rows.closed = true

	if rows.logger == dlogger {
		return
	}

	if rows.err == nil {
		endTime := time.Now()
		rows.logger.Info("Query", "sql", rows.sql, "args", logQueryArgs(rows.args), "time", endTime.Sub(rows.startTime), "rowCount", rows.rowCount)
	} else {
		rows.logger.Error("Query", "sql", rows.sql, "args", logQueryArgs(rows.args))
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

// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types and the Scanner interface.
func (rows *Rows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = fmt.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.fields))
		rows.Fatal(err)
		return err
	}

	for _, d := range dest {
		vr, _ := rows.nextColumn()
		switch d := d.(type) {
		case *bool:
			*d = decodeBool(vr)
		case *[]byte:
			// If it actually is a bytea then pass it through decodeBytea (so it can be decoded if it is in text format)
			// Otherwise read the bytes directly regardless of what the actual type is.
			if vr.Type().DataType == ByteaOid {
				*d = decodeBytea(vr)
			} else {
				if vr.Len() != -1 {
					*d = vr.ReadBytes(vr.Len())
				} else {
					*d = nil
				}
			}
		case *int64:
			*d = decodeInt8(vr)
		case *int16:
			*d = decodeInt2(vr)
		case *int32:
			*d = decodeInt4(vr)
		case *Oid:
			*d = decodeOid(vr)
		case *string:
			*d = decodeText(vr)
		case *float32:
			*d = decodeFloat4(vr)
		case *float64:
			*d = decodeFloat8(vr)
		case *[]bool:
			*d = decodeBoolArray(vr)
		case *[]int16:
			*d = decodeInt2Array(vr)
		case *[]int32:
			*d = decodeInt4Array(vr)
		case *[]int64:
			*d = decodeInt8Array(vr)
		case *[]float32:
			*d = decodeFloat4Array(vr)
		case *[]float64:
			*d = decodeFloat8Array(vr)
		case *[]string:
			*d = decodeTextArray(vr)
		case *[]time.Time:
			*d = decodeTimestampArray(vr)
		case *time.Time:
			switch vr.Type().DataType {
			case DateOid:
				*d = decodeDate(vr)
			case TimestampTzOid:
				*d = decodeTimestampTz(vr)
			case TimestampOid:
				*d = decodeTimestamp(vr)
			default:
				rows.Fatal(fmt.Errorf("Can't convert OID %v to time.Time", vr.Type().DataType))
			}
		case Scanner:
			err = d.Scan(vr)
			if err != nil {
				rows.Fatal(err)
			}
		default:
			rows.Fatal(fmt.Errorf("Scan cannot decode into %T", d))
		}

		if vr.Err() != nil {
			rows.Fatal(vr.Err())
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

	for _, _ = range rows.fields {
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

// Query executes sql with args. If there is an error the returned *Rows will
// be returned in an error state. So it is allowed to ignore the error returned
// from Query and handle it in *Rows.
func (c *Conn) Query(sql string, args ...interface{}) (*Rows, error) {
	c.lastActivityTime = time.Now()
	rows := &Rows{conn: c, startTime: c.lastActivityTime, sql: sql, args: args, logger: c.logger}

	ps, ok := c.preparedStatements[sql]
	if !ok {
		var err error
		ps, err = c.Prepare("", sql)
		if err != nil {
			rows.abort(err)
			return rows, rows.err
		}
	}

	rows.fields = ps.FieldDescriptions
	err := c.sendPreparedQuery(ps, args...)
	if err != nil {
		rows.abort(err)
	}
	return rows, rows.err
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned *Row. That *Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := c.Query(sql, args...)
	return (*Row)(rows)
}
