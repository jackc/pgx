package pgx

import (
	"errors"
	"fmt"
	"time"
)

type Row Rows

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

type Rows struct {
	pool      *ConnPool
	conn      *Conn
	mr        *MsgReader
	fields    []FieldDescription
	rowCount  int
	columnIdx int
	err       error
	closed    bool
}

func (rows *Rows) FieldDescriptions() []FieldDescription {
	return rows.fields
}

func (rows *Rows) MsgReader() *MsgReader {
	return rows.mr
}

func (rows *Rows) close() {
	if rows.pool != nil {
		rows.pool.Release(rows.conn)
		rows.pool = nil
	}

	rows.closed = true
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

// Fatal signals an error occurred after the query was sent to the server
func (rows *Rows) Fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

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
			fieldCount := r.ReadInt16()
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

func (rows *Rows) nextColumn() (*FieldDescription, int32, bool) {
	if rows.closed {
		return nil, 0, false
	}
	if len(rows.fields) <= rows.columnIdx {
		rows.Fatal(ProtocolError("No next column available"))
		return nil, 0, false
	}

	fd := &rows.fields[rows.columnIdx]
	rows.columnIdx++
	size := rows.mr.ReadInt32()
	return fd, size, true
}

func (rows *Rows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = errors.New("Scan received wrong number of arguments")
		rows.Fatal(err)
		return err
	}

	for _, d := range dest {
		fd, size, _ := rows.nextColumn()
		switch d := d.(type) {
		case *bool:
			*d = decodeBool(rows, fd, size)
		case *[]byte:
			*d = decodeBytea(rows, fd, size)
		case *int64:
			*d = decodeInt8(rows, fd, size)
		case *int16:
			*d = decodeInt2(rows, fd, size)
		case *int32:
			*d = decodeInt4(rows, fd, size)
		case *string:
			*d = decodeText(rows, fd, size)
		case *float32:
			*d = decodeFloat4(rows, fd, size)
		case *float64:
			*d = decodeFloat8(rows, fd, size)
		case *time.Time:
			switch fd.DataType {
			case DateOid:
				*d = decodeDate(rows, fd, size)
			case TimestampTzOid:
				*d = decodeTimestampTz(rows, fd, size)
			case TimestampOid:
				*d = decodeTimestamp(rows, fd, size)
			default:
				err = fmt.Errorf("Can't convert OID %v to time.Time", fd.DataType)
				rows.Fatal(err)
				return err
			}

		case Scanner:
			err = d.Scan(rows, fd, size)
			if err != nil {
				return err
			}
		default:
			return errors.New("Unknown type")
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
		if rows.Err() != nil {
			return nil, rows.Err()
		}

		fd, size, _ := rows.nextColumn()

		switch fd.DataType {
		case BoolOid:
			values = append(values, decodeBool(rows, fd, size))
		case ByteaOid:
			values = append(values, decodeBytea(rows, fd, size))
		case Int8Oid:
			values = append(values, decodeInt8(rows, fd, size))
		case Int2Oid:
			values = append(values, decodeInt2(rows, fd, size))
		case Int4Oid:
			values = append(values, decodeInt4(rows, fd, size))
		case VarcharOid, TextOid:
			values = append(values, decodeText(rows, fd, size))
		case Float4Oid:
			values = append(values, decodeFloat4(rows, fd, size))
		case Float8Oid:
			values = append(values, decodeFloat8(rows, fd, size))
		case DateOid:
			values = append(values, decodeDate(rows, fd, size))
		case TimestampTzOid:
			values = append(values, decodeTimestampTz(rows, fd, size))
		case TimestampOid:
			values = append(values, decodeTimestamp(rows, fd, size))
		default:
			// if it is not an intrinsic type then return the text
			switch fd.FormatCode {
			case TextFormatCode:
				values = append(values, rows.MsgReader().ReadString(size))
			case BinaryFormatCode:
				return nil, errors.New("Values cannot handle binary format non-intrinsic types")
			default:
				return nil, errors.New("Unknown format code")
			}
		}
	}

	return values, rows.Err()
}

// TODO - document
func (c *Conn) Query(sql string, args ...interface{}) (*Rows, error) {
	c.rows = Rows{conn: c}
	rows := &c.rows

	if ps, present := c.preparedStatements[sql]; present {
		rows.fields = ps.FieldDescriptions
		err := c.sendPreparedQuery(ps, args...)
		if err != nil {
			rows.abort(err)
		}
		return rows, rows.err
	}

	err := c.sendSimpleQuery(sql, args...)
	if err != nil {
		rows.abort(err)
		return rows, rows.err
	}

	// Simple queries don't know the field descriptions of the result.
	// Read until that is known before returning
	for {
		t, r, err := c.rxMsg()
		if err != nil {
			rows.Fatal(err)
			return rows, rows.err
		}

		switch t {
		case rowDescription:
			rows.fields = rows.conn.rxRowDescription(r)
			return rows, nil
		default:
			err = rows.conn.processContextFreeMsg(t, r)
			if err != nil {
				rows.Fatal(err)
				return rows, rows.err
			}
		}
	}
}

func (c *Conn) QueryRow(sql string, args ...interface{}) *Row {
	rows, _ := c.Query(sql, args...)
	return (*Row)(rows)
}
