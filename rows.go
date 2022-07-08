package pgx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
//
// Once a Rows is closed the only methods that may be called are Close(), Err(), and CommandTag().
//
// Rows is an interface instead of a struct to allow tests to mock Query. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Rows interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while reading.
	Err() error

	// CommandTag returns the command tag from this query. It is only available after Rows is closed.
	CommandTag() pgconn.CommandTag

	FieldDescriptions() []pgproto3.FieldDescription

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available. It automatically closes rows
	// when all rows are read.
	Next() bool

	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, and nil. nil will skip the value entirely. It is an error to
	// call Scan without first calling Next() and checking that it returned true.
	Scan(dest ...any) error

	// Values returns the decoded row values. As with Scan(), it is an error to
	// call Values without first calling Next() and checking that it returned
	// true.
	Values() ([]any, error)

	// RawValues returns the unparsed bytes of the row values. The returned data is only valid until the next Next
	// call or the Rows is closed.
	RawValues() [][]byte
}

// Row is a convenience wrapper over Rows that is returned by QueryRow.
//
// Row is an interface instead of a struct to allow tests to mock QueryRow. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Row interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Row interface {
	// Scan works the same as Rows. with the following exceptions. If no
	// rows were found it returns ErrNoRows. If multiple rows are returned it
	// ignores all but the first.
	Scan(dest ...any) error
}

// RowScanner scans an entire row at a time into the RowScanner.
type RowScanner interface {
	// ScanRows scans the row.
	ScanRow(rows Rows) error
}

// connRow implements the Row interface for Conn.QueryRow.
type connRow baseRows

func (r *connRow) Scan(dest ...any) (err error) {
	rows := (*baseRows)(r)

	if rows.Err() != nil {
		return rows.Err()
	}

	for _, d := range dest {
		if _, ok := d.(*pgtype.DriverBytes); ok {
			rows.Close()
			return fmt.Errorf("cannot scan into *pgtype.DriverBytes from QueryRow")
		}
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
	log(ctx context.Context, lvl LogLevel, msg string, data map[string]any)
}

// baseRows implements the Rows interface for Conn.Query.
type baseRows struct {
	typeMap      *pgtype.Map
	resultReader *pgconn.ResultReader

	values [][]byte

	commandTag pgconn.CommandTag
	err        error
	closed     bool

	scanPlans []pgtype.ScanPlan
	scanTypes []reflect.Type

	conn              *Conn
	multiResultReader *pgconn.MultiResultReader

	logger    rowLog
	ctx       context.Context
	startTime time.Time
	sql       string
	args      []any
	rowCount  int
}

func (rows *baseRows) FieldDescriptions() []pgproto3.FieldDescription {
	return rows.resultReader.FieldDescriptions()
}

func (rows *baseRows) Close() {
	if rows.closed {
		return
	}

	rows.closed = true

	if rows.resultReader != nil {
		var closeErr error
		rows.commandTag, closeErr = rows.resultReader.Close()
		if rows.err == nil {
			rows.err = closeErr
		}
	}

	if rows.multiResultReader != nil {
		closeErr := rows.multiResultReader.Close()
		if rows.err == nil {
			rows.err = closeErr
		}
	}

	if rows.logger != nil {
		if rows.err == nil {
			if rows.logger.shouldLog(LogLevelInfo) {
				endTime := time.Now()
				rows.logger.log(rows.ctx, LogLevelInfo, "Query", map[string]any{"sql": rows.sql, "args": logQueryArgs(rows.args), "time": endTime.Sub(rows.startTime), "rowCount": rows.rowCount})
			}
		} else {
			if rows.logger.shouldLog(LogLevelError) {
				rows.logger.log(rows.ctx, LogLevelError, "Query", map[string]any{"err": rows.err, "sql": rows.sql, "args": logQueryArgs(rows.args)})
			}
		}
	}

	if rows.err != nil && rows.conn != nil && rows.conn.statementCache != nil {
		rows.conn.statementCache.StatementErrored(rows.sql, rows.err)
	}
}

func (rows *baseRows) CommandTag() pgconn.CommandTag {
	return rows.commandTag
}

func (rows *baseRows) Err() error {
	return rows.err
}

// fatal signals an error occurred after the query was sent to the server. It
// closes the rows automatically.
func (rows *baseRows) fatal(err error) {
	if rows.err != nil {
		return
	}

	rows.err = err
	rows.Close()
}

func (rows *baseRows) Next() bool {
	if rows.closed {
		return false
	}

	if rows.resultReader.NextRow() {
		rows.rowCount++
		rows.values = rows.resultReader.Values()
		return true
	} else {
		rows.Close()
		return false
	}
}

func (rows *baseRows) Scan(dest ...any) error {
	m := rows.typeMap
	fieldDescriptions := rows.FieldDescriptions()
	values := rows.values

	if len(fieldDescriptions) != len(values) {
		err := fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(values))
		rows.fatal(err)
		return err
	}

	if len(dest) == 1 {
		if rc, ok := dest[0].(RowScanner); ok {
			return rc.ScanRow(rows)
		}
	}

	if len(fieldDescriptions) != len(dest) {
		err := fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
		rows.fatal(err)
		return err
	}

	if rows.scanPlans == nil {
		rows.scanPlans = make([]pgtype.ScanPlan, len(values))
		rows.scanTypes = make([]reflect.Type, len(values))
		for i := range dest {
			rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			rows.scanTypes[i] = reflect.TypeOf(dest[i])
		}
	}

	for i, dst := range dest {
		if dst == nil {
			continue
		}

		if rows.scanTypes[i] != reflect.TypeOf(dst) {
			rows.scanPlans[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			rows.scanTypes[i] = reflect.TypeOf(dest[i])
		}

		err := rows.scanPlans[i].Scan(values[i], dst)
		if err != nil {
			err = ScanArgError{ColumnIndex: i, Err: err}
			rows.fatal(err)
			return err
		}
	}

	return nil
}

func (rows *baseRows) Values() ([]any, error) {
	if rows.closed {
		return nil, errors.New("rows is closed")
	}

	values := make([]any, 0, len(rows.FieldDescriptions()))

	for i := range rows.FieldDescriptions() {
		buf := rows.values[i]
		fd := &rows.FieldDescriptions()[i]

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.typeMap.TypeForOID(fd.DataTypeOID); ok {
			value, err := dt.Codec.DecodeValue(rows.typeMap, fd.DataTypeOID, fd.Format, buf)
			if err != nil {
				rows.fatal(err)
			}
			values = append(values, value)
		} else {
			switch fd.Format {
			case TextFormatCode:
				values = append(values, string(buf))
			case BinaryFormatCode:
				newBuf := make([]byte, len(buf))
				copy(newBuf, buf)
				values = append(values, newBuf)
			default:
				rows.fatal(errors.New("Unknown format code"))
			}
		}

		if rows.Err() != nil {
			return nil, rows.Err()
		}
	}

	return values, rows.Err()
}

func (rows *baseRows) RawValues() [][]byte {
	return rows.values
}

type ScanArgError struct {
	ColumnIndex int
	Err         error
}

func (e ScanArgError) Error() string {
	return fmt.Sprintf("can't scan into dest[%d]: %v", e.ColumnIndex, e.Err)
}

func (e ScanArgError) Unwrap() error {
	return e.Err
}

// ScanRow decodes raw row data into dest. It can be used to scan rows read from the lower level pgconn interface.
//
// typeMap - OID to Go type mapping.
// fieldDescriptions - OID and format of values
// values - the raw data as returned from the PostgreSQL server
// dest - the destination that values will be decoded into
func ScanRow(typeMap *pgtype.Map, fieldDescriptions []pgproto3.FieldDescription, values [][]byte, dest ...any) error {
	if len(fieldDescriptions) != len(values) {
		return fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(values))
	}
	if len(fieldDescriptions) != len(dest) {
		return fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
	}

	for i, d := range dest {
		if d == nil {
			continue
		}

		err := typeMap.Scan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, values[i], d)
		if err != nil {
			return ScanArgError{ColumnIndex: i, Err: err}
		}
	}

	return nil
}

// RowsFromResultReader returns a Rows that will read from values resultReader and decode with typeMap. It can be used
// to read from the lower level pgconn interface.
func RowsFromResultReader(typeMap *pgtype.Map, resultReader *pgconn.ResultReader) Rows {
	return &baseRows{
		typeMap:      typeMap,
		resultReader: resultReader,
	}
}

// ForEachScannedRow iterates through rows. For each row it scans into the elements of scans and calls fn. If any row
// fails to scan or fn returns an error the query will be aborted and the error will be returned. Rows will be closed
// when ForEachScannedRow returns.
func ForEachScannedRow(rows Rows, scans []any, fn func() error) (pgconn.CommandTag, error) {
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		err = fn()
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}

	if err := rows.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}

	return rows.CommandTag(), nil
}
