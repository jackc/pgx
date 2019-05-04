package pgx

import (
	"fmt"
	"reflect"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	Err() error
	FieldDescriptions() []pgproto3.FieldDescription

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available. It automatically closes rows
	// when all rows are read.
	Next() bool

	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, []byte, and nil. []byte will skip the decoding process and directly
	// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
	Scan(dest ...interface{}) error

	// Values returns an array of the row values
	Values() ([]interface{}, error)
}

// Row is a convenience wrapper over Rows that is returned by QueryRow.
type Row interface {
	// Scan works the same as Rows. with the following exceptions. If no
	// rows were found it returns ErrNoRows. If multiple rows are returned it
	// ignores all but the first.
	Scan(dest ...interface{}) error
}

// connRow implements the Row interface for Conn.QueryRow.
type connRow connRows

func (r *connRow) Scan(dest ...interface{}) (err error) {
	rows := (*connRows)(r)

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

type rowLog interface {
	shouldLog(lvl LogLevel) bool
	log(lvl LogLevel, msg string, data map[string]interface{})
}

// connRows implements the Rows interface for Conn.Query.
type connRows struct {
	logger    rowLog
	connInfo  *pgtype.ConnInfo
	values    [][]byte
	rowCount  int
	columnIdx int
	err       error
	startTime time.Time
	sql       string
	args      []interface{}
	closed    bool

	resultReader *pgconn.ResultReader
}

func (rows *connRows) FieldDescriptions() []pgproto3.FieldDescription {
	return rows.resultReader.FieldDescriptions()
}

func (rows *connRows) Close() {
	if rows.closed {
		return
	}

	rows.closed = true

	if rows.resultReader != nil {
		_, closeErr := rows.resultReader.Close()
		if rows.err == nil {
			rows.err = closeErr
		}
	}

	if rows.logger != nil {
		if rows.err == nil {
			if rows.logger.shouldLog(LogLevelInfo) {
				endTime := time.Now()
				rows.logger.log(LogLevelInfo, "Query", map[string]interface{}{"sql": rows.sql, "args": logQueryArgs(rows.args), "time": endTime.Sub(rows.startTime), "rowCount": rows.rowCount})
			}
		} else if rows.logger.shouldLog(LogLevelError) {
			rows.logger.log(LogLevelError, "Query", map[string]interface{}{"sql": rows.sql, "args": logQueryArgs(rows.args)})
		}
	}
}

func (rows *connRows) Err() error {
	return rows.err
}

// fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *connRows) fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

func (rows *connRows) Next() bool {
	if rows.closed {
		return false
	}

	if rows.resultReader.NextRow() {
		rows.rowCount++
		rows.columnIdx = 0
		rows.values = rows.resultReader.Values()
		return true
	} else {
		rows.Close()
		return false
	}
}

func (rows *connRows) nextColumn() ([]byte, *pgproto3.FieldDescription, bool) {
	if rows.closed {
		return nil, nil, false
	}
	if len(rows.FieldDescriptions()) <= rows.columnIdx {
		rows.fatal(ProtocolError("No next column available"))
		return nil, nil, false
	}

	buf := rows.values[rows.columnIdx]
	fd := &rows.FieldDescriptions()[rows.columnIdx]
	rows.columnIdx++
	return buf, fd, true
}

func (rows *connRows) Scan(dest ...interface{}) error {
	if len(rows.FieldDescriptions()) != len(dest) {
		err := errors.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.FieldDescriptions()))
		rows.fatal(err)
		return err
	}

	for i, d := range dest {
		buf, fd, _ := rows.nextColumn()

		if d == nil {
			continue
		}

		err := rows.connInfo.Scan(pgtype.OID(fd.DataTypeOID), fd.Format, buf, d)
		if err != nil {
			rows.fatal(scanArgError{col: i, err: err})
			return err
		}
	}

	return nil
}

func (rows *connRows) Values() ([]interface{}, error) {
	if rows.closed {
		return nil, errors.New("rows is closed")
	}

	values := make([]interface{}, 0, len(rows.FieldDescriptions()))

	for range rows.FieldDescriptions() {
		buf, fd, _ := rows.nextColumn()

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.connInfo.DataTypeForOID(pgtype.OID(fd.DataTypeOID)); ok {
			value := reflect.New(reflect.ValueOf(dt.Value).Elem().Type()).Interface().(pgtype.Value)

			switch fd.Format {
			case TextFormatCode:
				decoder := value.(pgtype.TextDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericText{}
				}
				err := decoder.DecodeText(rows.connInfo, buf)
				if err != nil {
					rows.fatal(err)
				}
				values = append(values, decoder.(pgtype.Value).Get())
			case BinaryFormatCode:
				decoder := value.(pgtype.BinaryDecoder)
				if decoder == nil {
					decoder = &pgtype.GenericBinary{}
				}
				err := decoder.DecodeBinary(rows.connInfo, buf)
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

type scanArgError struct {
	col int
	err error
}

func (e scanArgError) Error() string {
	return fmt.Sprintf("can't scan into dest[%d]: %v", e.col, e.err)
}

// RowsFromResultReader wraps a *pgconn.ResultReader in a Rows wrapper so a more convenient scanning interface can be
// used.
//
// In most cases, the appropriate pgx query methods should be used instead of sending a query with pgconn and reading
// the results with pgx.
func RowsFromResultReader(connInfo *pgtype.ConnInfo, rr *pgconn.ResultReader) Rows {
	return &connRows{
		connInfo:     connInfo,
		resultReader: rr,
	}
}
