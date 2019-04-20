package pgx

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/pgtype"
)

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	Err() error
	FieldDescriptions() []FieldDescription

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

// connRows implements the Rows interface for Conn.Query.
type connRows struct {
	conn      *Conn
	batch     *Batch
	values    [][]byte
	fields    []FieldDescription
	rowCount  int
	columnIdx int
	err       error
	startTime time.Time
	sql       string
	args      []interface{}
	closed    bool

	resultReader      *pgconn.ResultReader
	multiResultReader *pgconn.MultiResultReader
}

func (rows *connRows) FieldDescriptions() []FieldDescription {
	return rows.fields
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

	if rows.multiResultReader != nil {
		closeErr := rows.multiResultReader.Close()
		if rows.err == nil {
			rows.err = closeErr
		}
	}

	if rows.err == nil {
		if rows.conn.shouldLog(LogLevelInfo) {
			endTime := time.Now()
			rows.conn.log(LogLevelInfo, "Query", map[string]interface{}{"sql": rows.sql, "args": logQueryArgs(rows.args), "time": endTime.Sub(rows.startTime), "rowCount": rows.rowCount})
		}
	} else if rows.conn.shouldLog(LogLevelError) {
		rows.conn.log(LogLevelError, "Query", map[string]interface{}{"sql": rows.sql, "args": logQueryArgs(rows.args)})
	}

	if rows.batch != nil && rows.err != nil {
		rows.batch.die(rows.err)
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
		if rows.fields == nil {
			rrFieldDescriptions := rows.resultReader.FieldDescriptions()
			rows.fields = make([]FieldDescription, len(rrFieldDescriptions))
			for i := range rrFieldDescriptions {
				rows.conn.pgproto3FieldDescriptionToPgxFieldDescription(&rrFieldDescriptions[i], &rows.fields[i])
			}
		}
		rows.rowCount++
		rows.columnIdx = 0
		rows.values = rows.resultReader.Values()
		return true
	} else {
		rows.Close()
		return false
	}
}

func (rows *connRows) nextColumn() ([]byte, *FieldDescription, bool) {
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

func (rows *connRows) Scan(dest ...interface{}) error {
	if len(rows.fields) != len(dest) {
		err := errors.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.fields))
		rows.fatal(err)
		return err
	}

	for i, d := range dest {
		buf, fd, _ := rows.nextColumn()

		if d == nil {
			continue
		}

		err := rows.conn.ConnInfo.Scan(fd.DataType, fd.FormatCode, buf, d)
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

	values := make([]interface{}, 0, len(rows.fields))

	for range rows.fields {
		buf, fd, _ := rows.nextColumn()

		if buf == nil {
			values = append(values, nil)
			continue
		}

		if dt, ok := rows.conn.ConnInfo.DataTypeForOID(fd.DataType); ok {
			value := reflect.New(reflect.ValueOf(dt.Value).Elem().Type()).Interface().(pgtype.Value)

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

type scanArgError struct {
	col int
	err error
}

func (e scanArgError) Error() string {
	return fmt.Sprintf("can't scan into dest[%d]: %v", e.col, e.err)
}

func (c *Conn) getRows(sql string, args []interface{}) *connRows {
	if len(c.preallocatedRows) == 0 {
		c.preallocatedRows = make([]connRows, 64)
	}

	r := &c.preallocatedRows[len(c.preallocatedRows)-1]
	c.preallocatedRows = c.preallocatedRows[0 : len(c.preallocatedRows)-1]

	r.conn = c
	r.startTime = time.Now()
	r.sql = sql
	r.args = args

	return r
}

type QueryResultFormats []int16

// Query executes sql with args. If there is an error the returned Rows will be returned in an error state. So it is
// allowed to ignore the error returned from Query and handle it in Rows.
func (c *Conn) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	c.lastStmtSent = false
	// rows = c.getRows(sql, args)

	var resultFormats QueryResultFormats

optionLoop:
	for len(args) > 0 {
		switch arg := args[0].(type) {
		case QueryResultFormats:
			resultFormats = arg
			args = args[1:]
		default:
			break optionLoop
		}
	}

	rows := &connRows{
		conn:      c,
		startTime: time.Now(),
		sql:       sql,
		args:      args,
	}

	ps, ok := c.preparedStatements[sql]
	if !ok {
		psd, err := c.pgConn.Prepare(ctx, "", sql, nil)
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}

		if len(psd.ParamOIDs) != len(args) {
			rows.fatal(errors.Errorf("expected %d arguments, got %d", len(psd.ParamOIDs), len(args)))
			return rows, rows.err
		}

		ps = &PreparedStatement{
			Name:              psd.Name,
			SQL:               psd.SQL,
			ParameterOIDs:     make([]pgtype.OID, len(psd.ParamOIDs)),
			FieldDescriptions: make([]FieldDescription, len(psd.Fields)),
		}

		for i := range ps.ParameterOIDs {
			ps.ParameterOIDs[i] = pgtype.OID(psd.ParamOIDs[i])
		}
		for i := range ps.FieldDescriptions {
			c.pgproto3FieldDescriptionToPgxFieldDescription(&psd.Fields[i], &ps.FieldDescriptions[i])
		}
	}
	rows.sql = ps.SQL

	var err error
	args, err = convertDriverValuers(args)
	if err != nil {
		rows.fatal(err)
		return rows, rows.err
	}

	paramFormats := make([]int16, len(args))
	paramValues := make([][]byte, len(args))
	for i := range args {
		paramFormats[i] = chooseParameterFormatCode(c.ConnInfo, ps.ParameterOIDs[i], args[i])
		paramValues[i], err = newencodePreparedStatementArgument(c.ConnInfo, ps.ParameterOIDs[i], args[i])
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}
	}

	if resultFormats == nil {
		resultFormats = make([]int16, len(ps.FieldDescriptions))
		for i := range resultFormats {
			if dt, ok := c.ConnInfo.DataTypeForOID(ps.FieldDescriptions[i].DataType); ok {
				if _, ok := dt.Value.(pgtype.BinaryDecoder); ok {
					resultFormats[i] = BinaryFormatCode
				} else {
					resultFormats[i] = TextFormatCode
				}
			}
		}
	}

	c.lastStmtSent = true
	rows.resultReader = c.pgConn.ExecPrepared(ctx, ps.Name, paramValues, paramFormats, resultFormats)

	return rows, rows.err
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned Row. That Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	rows, _ := c.Query(ctx, sql, args...)
	return (*connRow)(rows.(*connRows))
}
