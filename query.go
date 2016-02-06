package pgx

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"reflect"
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
	pool       *ConnPool
	conn       Conn
	mr         *msgReader
	fields     []FieldDescription
	vr         ValueReader
	rowCount   int
	columnIdx  int
	err        error
	closed     bool
	startTime  time.Time
	sql        string
	args       []interface{}
	logger     Logger
	logLevel   int
	unlockConn bool
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

	if rows.pool != nil {
		rows.pool.Release(rows.conn)
		rows.pool = nil
	}

	rows.closed = true

	if rows.err == nil {
		if rows.logLevel >= LogLevelInfo {
			endTime := time.Now()
			rows.logger.Info("Query", "sql", rows.sql, "args", logQueryArgs(rows.args), "time", endTime.Sub(rows.startTime), "rowCount", rows.rowCount)
		}
	} else if rows.logLevel >= LogLevelError {
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
// interface, and []byte. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL.
func (rows *Rows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = fmt.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.fields))
		rows.Fatal(err)
		return err
	}

	for i, d := range dest {
		vr, _ := rows.nextColumn()

		// Check for []byte first as we allow sidestepping the decoding process and retrieving the raw bytes
		if b, ok := d.(*[]byte); ok {
			// If it actually is a bytea then pass it through decodeBytea (so it can be decoded if it is in text format)
			// Otherwise read the bytes directly regardless of what the actual type is.
			if vr.Type().DataType == ByteaOID {
				*b = DecodeBytea(vr)
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
				case BoolOID:
					val = DecodeBool(vr)
				case Int8OID:
					val = int64(DecodeInt8(vr))
				case Int2OID:
					val = int64(DecodeInt2(vr))
				case Int4OID:
					val = int64(DecodeInt4(vr))
				case TextOID, VarcharOID:
					val = DecodeText(vr)
				case OIDOID:
					val = int64(DecodeOID(vr))
				case Float4OID:
					val = float64(DecodeFloat4(vr))
				case Float8OID:
					val = DecodeFloat8(vr)
				case DateOID:
					val = DecodeDate(vr)
				case TimestampOID:
					val = DecodeTimestamp(vr)
				case TimestampTzOID:
					val = DecodeTimestampTz(vr)
				default:
					val = vr.ReadBytes(vr.Len())
				}
			}
			err = s.Scan(val)
			if err != nil {
				rows.Fatal(scanArgError{col: i, err: err})
			}
		} else if vr.Type().DataType == JsonOID || vr.Type().DataType == JsonbOID {
			decodeJson(vr, &d)
		} else {
		decode:
			switch v := d.(type) {
			case *bool:
				*v = DecodeBool(vr)
			case *int64:
				*v = DecodeInt8(vr)
			case *int16:
				*v = DecodeInt2(vr)
			case *int32:
				*v = DecodeInt4(vr)
			case *OID:
				*v = DecodeOID(vr)
			case *string:
				*v = DecodeText(vr)
			case *float32:
				*v = DecodeFloat4(vr)
			case *float64:
				*v = DecodeFloat8(vr)
			case *[]bool:
				*v = DecodeBoolArray(vr)
			case *[]int16:
				*v = DecodeInt2Array(vr)
			case *[]int32:
				*v = DecodeInt4Array(vr)
			case *[]int64:
				*v = DecodeInt8Array(vr)
			case *[]float32:
				*v = DecodeFloat4Array(vr)
			case *[]float64:
				*v = DecodeFloat8Array(vr)
			case *[]string:
				*v = DecodeTextArray(vr)
			case *[]time.Time:
				*v = DecodeTimestampArray(vr)
			case *time.Time:
				switch vr.Type().DataType {
				case DateOID:
					*v = DecodeDate(vr)
				case TimestampTzOID:
					*v = DecodeTimestampTz(vr)
				case TimestampOID:
					*v = DecodeTimestamp(vr)
				default:
					rows.Fatal(scanArgError{col: i, err: fmt.Errorf("Can't convert OID %v to time.Time", vr.Type().DataType)})
				}
			case *net.IPNet:
				*v = DecodeInet(vr)
			case *[]net.IPNet:
				*v = DecodeInetArray(vr)
			default:
				// if d is a pointer to pointer, strip the pointer and try again
				if v := reflect.ValueOf(d); v.Kind() == reflect.Ptr {
					if el := v.Elem(); el.Kind() == reflect.Ptr {
						// -1 is a null value
						if vr.Len() == -1 {
							if !el.IsNil() {
								// if the destination pointer is not nil, nil it out
								el.Set(reflect.Zero(el.Type()))
							}
							continue
						} else {
							if el.IsNil() {
								// allocate destination
								el.Set(reflect.New(el.Type().Elem()))
							}
							d = el.Interface()
							goto decode
						}
					}
				}
				rows.Fatal(scanArgError{col: i, err: fmt.Errorf("Scan cannot decode into %T", d)})
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
			case BoolOID:
				values = append(values, DecodeBool(vr))
			case ByteaOID:
				values = append(values, DecodeBytea(vr))
			case Int8OID:
				values = append(values, DecodeInt8(vr))
			case Int2OID:
				values = append(values, DecodeInt2(vr))
			case Int4OID:
				values = append(values, DecodeInt4(vr))
			case OIDOID:
				values = append(values, DecodeOID(vr))
			case Float4OID:
				values = append(values, DecodeFloat4(vr))
			case Float8OID:
				values = append(values, DecodeFloat8(vr))
			case BoolArrayOID:
				values = append(values, DecodeBoolArray(vr))
			case Int2ArrayOID:
				values = append(values, DecodeInt2Array(vr))
			case Int4ArrayOID:
				values = append(values, DecodeInt4Array(vr))
			case Int8ArrayOID:
				values = append(values, DecodeInt8Array(vr))
			case Float4ArrayOID:
				values = append(values, DecodeFloat4Array(vr))
			case Float8ArrayOID:
				values = append(values, DecodeFloat8Array(vr))
			case TextArrayOID, VarcharArrayOID:
				values = append(values, DecodeTextArray(vr))
			case TimestampArrayOID, TimestampTzArrayOID:
				values = append(values, DecodeTimestampArray(vr))
			case DateOID:
				values = append(values, DecodeDate(vr))
			case TimestampTzOID:
				values = append(values, DecodeTimestampTz(vr))
			case TimestampOID:
				values = append(values, DecodeTimestamp(vr))
			case InetOID, CidrOID:
				values = append(values, DecodeInet(vr))
			case JsonOID:
				var d interface{}
				decodeJson(vr, &d)
				values = append(values, d)
			case JsonbOID:
				var d interface{}
				decodeJson(vr, &d)
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

// Query executes sql with args. If there is an error the returned *Rows will
// be returned in an error state. So it is allowed to ignore the error returned
// from Query and handle it in *Rows.
func (c *TempNameConn) Query(sql string, args ...interface{}) (*Rows, error) {
	c.lastActivityTime = time.Now()
	rows := &Rows{conn: c, startTime: c.lastActivityTime, sql: sql, args: args, logger: c.logger, logLevel: c.logLevel}

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
func (c *TempNameConn) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := c.Query(sql, args...)
	return (*Row)(rows)
}
