package pgx

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/internal/sanitize"
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
	conn       *Conn
	batch      *Batch
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

	if rows.unlockConn {
		rows.conn.unlock()
		rows.unlockConn = false
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

func (rows *connRows) Scan(dest ...interface{}) (err error) {
	if len(rows.fields) != len(dest) {
		err = errors.Errorf("Scan received wrong number of arguments, got %d but expected %d", len(dest), len(rows.fields))
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
			if dt, ok := rows.conn.ConnInfo.DataTypeForOID(fd.DataType); ok {
				value := dt.Value
				switch fd.FormatCode {
				case TextFormatCode:
					if textDecoder, ok := value.(pgtype.TextDecoder); ok {
						err = textDecoder.DecodeText(rows.conn.ConnInfo, buf)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else {
						rows.fatal(scanArgError{col: i, err: errors.Errorf("%T is not a pgtype.TextDecoder", value)})
					}
				case BinaryFormatCode:
					if binaryDecoder, ok := value.(pgtype.BinaryDecoder); ok {
						err = binaryDecoder.DecodeBinary(rows.conn.ConnInfo, buf)
						if err != nil {
							rows.fatal(scanArgError{col: i, err: err})
						}
					} else {
						rows.fatal(scanArgError{col: i, err: errors.Errorf("%T is not a pgtype.BinaryDecoder", value)})
					}
				default:
					rows.fatal(scanArgError{col: i, err: errors.Errorf("unknown format code: %v", fd.FormatCode)})
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
				rows.fatal(scanArgError{col: i, err: errors.Errorf("unknown oid: %v", fd.DataType)})
			}
		}

		if rows.Err() != nil {
			return rows.Err()
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

type QueryExOptions struct {
	// When ParameterOIDs are present and the query is not a prepared statement,
	// then ParameterOIDs and ResultFormatCodes will be used to avoid an extra
	// network round-trip.
	ParameterOIDs     []pgtype.OID
	ResultFormatCodes []int16

	SimpleProtocol bool
}

// Query executes sql with args. If there is an error the returned Rows will be returned in an error state. So it is
// allowed to ignore the error returned from Query and handle it in Rows.
func (c *Conn) Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (Rows, error) {
	c.lastStmtSent = false
	// rows = c.getRows(sql, args)

	var options *QueryExOptions
	args := optionsAndArgs
	if len(optionsAndArgs) > 0 {
		if o, ok := optionsAndArgs[0].(*QueryExOptions); ok {
			options = o
			args = optionsAndArgs[1:]
		}
	}

	rows := &connRows{
		conn:      c,
		startTime: time.Now(),
		sql:       sql,
		args:      args,
	}

	if err := c.lock(); err != nil {
		rows.fatal(err)
		return rows, err
	}
	rows.unlockConn = true

	// err = c.initContext(ctx)
	// if err != nil {
	// 	rows.fatal(err)
	// 	return rows, rows.err
	// }

	var err error
	if (options == nil && c.config.PreferSimpleProtocol) || (options != nil && options.SimpleProtocol) {
		sql, err = c.sanitizeForSimpleQuery(sql, args...)
		if err != nil {
			rows.fatal(err)
			return rows, err
		}

		c.lastStmtSent = true
		rows.multiResultReader = c.pgConn.Exec(ctx, sql)
		if rows.multiResultReader.NextResult() {
			rows.resultReader = rows.multiResultReader.ResultReader()
		} else {
			err = rows.multiResultReader.Close()
			rows.fatal(err)
			return rows, err
		}

		return rows, nil
	}

	// if options != nil && len(options.ParameterOIDs) > 0 {

	// 	buf, err := c.buildOneRoundTripQueryEx(c.wbuf, sql, options, args)
	// 	if err != nil {
	// 		rows.fatal(err)
	// 		return rows, err
	// 	}

	// 	buf = appendSync(buf)

	// 	n, err := c.pgConn.Conn().Write(buf)
	// 	c.lastStmtSent = true
	// 	if err != nil && fatalWriteErr(n, err) {
	// 		rows.fatal(err)
	// 		c.die(err)
	// 		return rows, err
	// 	}
	// 	c.pendingReadyForQueryCount++

	// 	fieldDescriptions, err := c.readUntilRowDescription()
	// 	if err != nil {
	// 		rows.fatal(err)
	// 		return rows, err
	// 	}

	// 	if len(options.ResultFormatCodes) == 0 {
	// 		for i := range fieldDescriptions {
	// 			fieldDescriptions[i].FormatCode = TextFormatCode
	// 		}
	// 	} else if len(options.ResultFormatCodes) == 1 {
	// 		fc := options.ResultFormatCodes[0]
	// 		for i := range fieldDescriptions {
	// 			fieldDescriptions[i].FormatCode = fc
	// 		}
	// 	} else {
	// 		for i := range options.ResultFormatCodes {
	// 			fieldDescriptions[i].FormatCode = options.ResultFormatCodes[i]
	// 		}
	// 	}

	// 	rows.sql = sql
	// 	rows.fields = fieldDescriptions
	// 	return rows, nil
	// }

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

	resultFormats := make([]int16, len(ps.FieldDescriptions))
	for i := range resultFormats {
		if dt, ok := c.ConnInfo.DataTypeForOID(ps.FieldDescriptions[i].DataType); ok {
			if _, ok := dt.Value.(pgtype.BinaryDecoder); ok {
				resultFormats[i] = BinaryFormatCode
			} else {
				resultFormats[i] = TextFormatCode
			}
		}
	}

	c.lastStmtSent = true
	rows.resultReader = c.pgConn.ExecPrepared(ctx, ps.Name, paramValues, paramFormats, resultFormats)

	return rows, rows.err
}

func (c *Conn) sanitizeForSimpleQuery(sql string, args ...interface{}) (string, error) {
	if c.pgConn.ParameterStatus("standard_conforming_strings") != "on" {
		return "", errors.New("simple protocol queries must be run with standard_conforming_strings=on")
	}

	if c.pgConn.ParameterStatus("client_encoding") != "UTF8" {
		return "", errors.New("simple protocol queries must be run with client_encoding=UTF8")
	}

	var err error
	valueArgs := make([]interface{}, len(args))
	for i, a := range args {
		valueArgs[i], err = convertSimpleArgument(c.ConnInfo, a)
		if err != nil {
			return "", err
		}
	}

	return sanitize.SanitizeSQL(sql, valueArgs...)
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned Row. That Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) Row {
	rows, _ := c.Query(ctx, sql, optionsAndArgs...)
	return (*connRow)(rows.(*connRows))
}
