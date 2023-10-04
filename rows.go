package pgx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Rows is the result set returned from *Conn.Query. Rows must be closed before
// the *Conn can be used again. Rows are closed by explicitly calling Close(),
// calling Next() until it returns false, or when a fatal error occurs.
//
// Once a Rows is closed the only methods that may be called are Close(), Err(),
// and CommandTag().
//
// Rows is an interface instead of a struct to allow tests to mock Query. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Rows interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while reading. Err must only be called after the Rows is closed (either by
	// calling Close or by Next returning false). If it is called early it may return nil even if there was an error
	// executing the query.
	Err() error

	// CommandTag returns the command tag from this query. It is only available after Rows is closed.
	CommandTag() pgconn.CommandTag

	// FieldDescriptions returns the field descriptions of the columns. It may return nil. In particular this can occur
	// when there was an error executing the query.
	FieldDescriptions() []pgconn.FieldDescription

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available or a fatal error has occurred.
	// It automatically closes rows when all rows are read.
	//
	// Callers should check rows.Err() after rows.Next() returns false to detect
	// whether result-set reading ended prematurely due to an error. See
	// Conn.Query for details.
	//
	// For simpler error handling, consider using the higher-level pgx v5
	// CollectRows() and ForEachRow() helpers instead.
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

	// Conn returns the underlying *Conn on which the query was executed. This may return nil if Rows did not come from a
	// *Conn (e.g. if it was created by RowsFromResultReader)
	Conn() *Conn
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

	queryTracer QueryTracer
	batchTracer BatchTracer
	ctx         context.Context
	startTime   time.Time
	sql         string
	args        []any
	rowCount    int
}

func (rows *baseRows) FieldDescriptions() []pgconn.FieldDescription {
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

	if rows.err != nil && rows.conn != nil && rows.sql != "" {
		if sc := rows.conn.statementCache; sc != nil {
			sc.Invalidate(rows.sql)
		}

		if sc := rows.conn.descriptionCache; sc != nil {
			sc.Invalidate(rows.sql)
		}
	}

	if rows.batchTracer != nil {
		rows.batchTracer.TraceBatchQuery(rows.ctx, rows.conn, TraceBatchQueryData{SQL: rows.sql, Args: rows.args, CommandTag: rows.commandTag, Err: rows.err})
	} else if rows.queryTracer != nil {
		rows.queryTracer.TraceQueryEnd(rows.ctx, rows.conn, TraceQueryEndData{rows.commandTag, rows.err})
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
			err := rc.ScanRow(rows)
			if err != nil {
				rows.fatal(err)
			}
			return err
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
				rows.fatal(errors.New("unknown format code"))
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

func (rows *baseRows) Conn() *Conn {
	return rows.conn
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
func ScanRow(typeMap *pgtype.Map, fieldDescriptions []pgconn.FieldDescription, values [][]byte, dest ...any) error {
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

// ForEachRow iterates through rows. For each row it scans into the elements of scans and calls fn. If any row
// fails to scan or fn returns an error the query will be aborted and the error will be returned. Rows will be closed
// when ForEachRow returns.
func ForEachRow(rows Rows, scans []any, fn func() error) (pgconn.CommandTag, error) {
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

// CollectableRow is the subset of Rows methods that a RowToFunc is allowed to call.
type CollectableRow interface {
	FieldDescriptions() []pgconn.FieldDescription
	Scan(dest ...any) error
	Values() ([]any, error)
	RawValues() [][]byte
}

// RowToFunc is a function that scans or otherwise converts row to a T.
type RowToFunc[T any] func(row CollectableRow) (T, error)

// CollectRows iterates through rows, calling fn for each row, and collecting the results into a slice of T.
func CollectRows[T any](rows Rows, fn RowToFunc[T]) ([]T, error) {
	defer rows.Close()

	slice := []T{}

	for rows.Next() {
		value, err := fn(rows)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}

// CollectOneRow calls fn for the first row in rows and returns the result. If no rows are found returns an error where errors.Is(ErrNoRows) is true.
// CollectOneRow is to CollectRows as QueryRow is to Query.
func CollectOneRow[T any](rows Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()

	var value T
	var err error

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return value, err
		}
		return value, ErrNoRows
	}

	value, err = fn(rows)
	if err != nil {
		return value, err
	}

	rows.Close()
	return value, rows.Err()
}

// CollectExactlyOneRow calls fn for the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
func CollectExactlyOneRow[T any](rows Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
	)

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return value, err
		}

		return value, ErrNoRows
	}

	value, err = fn(rows)
	if err != nil {
		return value, err
	}

	if rows.Next() {
		var zero T

		return zero, ErrTooManyRows
	}

	return value, rows.Err()
}

// RowTo returns a T scanned from row.
func RowTo[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&value)
	return value, err
}

// RowTo returns a the address of a T scanned from row.
func RowToAddrOf[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&value)
	return &value, err
}

// RowToMap returns a map scanned from row.
func RowToMap(row CollectableRow) (map[string]any, error) {
	var value map[string]any
	err := row.Scan((*mapRowScanner)(&value))
	return value, err
}

type mapRowScanner map[string]any

func (rs *mapRowScanner) ScanRow(rows Rows) error {
	values, err := rows.Values()
	if err != nil {
		return err
	}

	*rs = make(mapRowScanner, len(values))

	for i := range values {
		(*rs)[string(rows.FieldDescriptions()[i].Name)] = values[i]
	}

	return nil
}

// RowToStructByPos returns a T scanned from row. T must be a struct. T must have the same number a public fields as row
// has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then the field will be
// ignored.
func RowToStructByPos[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&positionalStructRowScanner{ptrToStruct: &value})
	return value, err
}

// RowToAddrOfStructByPos returns the address of a T scanned from row. T must be a struct. T must have the same number a
// public fields as row has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then
// the field will be ignored.
func RowToAddrOfStructByPos[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&positionalStructRowScanner{ptrToStruct: &value})
	return &value, err
}

type positionalStructRowScanner struct {
	ptrToStruct any
}

func (rs *positionalStructRowScanner) ScanRow(rows Rows) error {
	dst := rs.ptrToStruct
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dst not a pointer")
	}

	dstElemValue := dstValue.Elem()
	scanTargets := rs.appendScanTargets(dstElemValue, nil)

	if len(rows.RawValues()) > len(scanTargets) {
		return fmt.Errorf("got %d values, but dst struct has only %d fields", len(rows.RawValues()), len(scanTargets))
	}

	return rows.Scan(scanTargets...)
}

func (rs *positionalStructRowScanner) appendScanTargets(dstElemValue reflect.Value, scanTargets []any) []any {
	dstElemType := dstElemValue.Type()

	if scanTargets == nil {
		scanTargets = make([]any, 0, dstElemType.NumField())
	}

	for i := 0; i < dstElemType.NumField(); i++ {
		sf := dstElemType.Field(i)
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			scanTargets = rs.appendScanTargets(dstElemValue.Field(i), scanTargets)
		} else if sf.PkgPath == "" {
			dbTag, _ := sf.Tag.Lookup(structTagKey)
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			scanTargets = append(scanTargets, dstElemValue.Field(i).Addr().Interface())
		}
	}

	return scanTargets
}

// RowToStructByName returns a T scanned from row. T must be a struct. T must have the same number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByName[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value})
	return value, err
}

// RowToAddrOfStructByName returns the address of a T scanned from row. T must be a struct. T must have the same number
// of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByName[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value})
	return &value, err
}

// RowToStructByNameLax returns a T scanned from row. T must be a struct. T must have greater than or equal number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByNameLax[T any](row CollectableRow) (T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value, lax: true})
	return value, err
}

// RowToAddrOfStructByNameLax returns the address of a T scanned from row. T must be a struct. T must have greater than or
// equal number of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByNameLax[T any](row CollectableRow) (*T, error) {
	var value T
	err := row.Scan(&namedStructRowScanner{ptrToStruct: &value, lax: true})
	return &value, err
}

type namedStructRowScanner struct {
	ptrToStruct any
	lax         bool
}

func (rs *namedStructRowScanner) ScanRow(rows Rows) error {
	dst := rs.ptrToStruct
	dstValue := reflect.ValueOf(dst)
	if dstValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dst not a pointer")
	}

	dstElemValue := dstValue.Elem()
	scanTargets, err := rs.appendScanTargets(dstElemValue, nil, rows.FieldDescriptions())
	if err != nil {
		return err
	}

	for i, t := range scanTargets {
		if t == nil {
			return fmt.Errorf("struct doesn't have corresponding row field %s", rows.FieldDescriptions()[i].Name)
		}
	}

	return rows.Scan(scanTargets...)
}

const structTagKey = "db"

func fieldPosByName(fldDescs []pgconn.FieldDescription, field string) (i int) {
	i = -1
	for i, desc := range fldDescs {
		if strings.EqualFold(desc.Name, field) {
			return i
		}
	}
	return
}

func (rs *namedStructRowScanner) appendScanTargets(dstElemValue reflect.Value, scanTargets []any, fldDescs []pgconn.FieldDescription) ([]any, error) {
	var err error
	dstElemType := dstElemValue.Type()

	if scanTargets == nil {
		scanTargets = make([]any, len(fldDescs))
	}

	for i := 0; i < dstElemType.NumField(); i++ {
		sf := dstElemType.Field(i)
		if sf.PkgPath != "" && !sf.Anonymous {
			// Field is unexported, skip it.
			continue
		}
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			scanTargets, err = rs.appendScanTargets(dstElemValue.Field(i), scanTargets, fldDescs)
			if err != nil {
				return nil, err
			}
		} else {
			dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
			if dbTagPresent {
				dbTag, _, _ = strings.Cut(dbTag, ",")
			}
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			colName := dbTag
			if !dbTagPresent {
				colName = sf.Name
			}
			fpos := fieldPosByName(fldDescs, colName)
			if fpos == -1 {
				if rs.lax {
					continue
				}
				return nil, fmt.Errorf("cannot find field %s in returned row", colName)
			}
			if fpos >= len(scanTargets) && !rs.lax {
				return nil, fmt.Errorf("cannot find field %s in returned row", colName)
			}
			scanTargets[fpos] = dstElemValue.Field(i).Addr().Interface()
		}
	}

	return scanTargets, err
}
