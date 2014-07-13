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
	vr        ValueReader
	rowCount  int
	columnIdx int
	err       error
	closed    bool
}

func (rows *Rows) FieldDescriptions() []FieldDescription {
	return rows.fields
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

func (rows *Rows) nextColumn() (*ValueReader, bool) {
	if rows.closed {
		return nil, false
	}
	if len(rows.fields) <= rows.columnIdx {
		rows.Fatal(ProtocolError("No next column available"))
		return nil, false
	}

	fd := &rows.fields[rows.columnIdx]
	rows.columnIdx++
	size := rows.mr.ReadInt32()
	rows.vr = ValueReader{mr: rows.mr, fd: fd, valueBytesRemaining: size}
	return &rows.vr, true
}

func (rows *Rows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = errors.New("Scan received wrong number of arguments")
		rows.Fatal(err)
		return err
	}

	// TODO - decodeX should return err and Scan should Fatal the rows
	for _, d := range dest {
		vr, _ := rows.nextColumn()
		switch d := d.(type) {
		case *bool:
			*d = decodeBool(vr)
		case *[]byte:
			*d = decodeBytea(vr)
		case *int64:
			*d = decodeInt8(vr)
		case *int16:
			*d = decodeInt2(vr)
		case *int32:
			*d = decodeInt4(vr)
		case *string:
			*d = decodeText(vr)
		case *float32:
			*d = decodeFloat4(vr)
		case *float64:
			*d = decodeFloat8(vr)
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
			rows.Fatal(errors.New("Unknown type"))
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
		case VarcharOid, TextOid:
			values = append(values, decodeText(vr))
		case Float4Oid:
			values = append(values, decodeFloat4(vr))
		case Float8Oid:
			values = append(values, decodeFloat8(vr))
		case DateOid:
			values = append(values, decodeDate(vr))
		case TimestampTzOid:
			values = append(values, decodeTimestampTz(vr))
		case TimestampOid:
			values = append(values, decodeTimestamp(vr))
		default:
			// if it is not an intrinsic type then return the text
			switch vr.Type().FormatCode {
			case TextFormatCode:
				values = append(values, vr.ReadString(vr.Len()))
			case BinaryFormatCode:
				rows.Fatal(errors.New("Values cannot handle binary format non-intrinsic types"))
			default:
				rows.Fatal(errors.New("Unknown format code"))
			}
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
