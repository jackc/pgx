package pgx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Rows is the result set returned from [Conn.Query]. Rows must be closed before
// the [Conn] can be used again. Rows are closed by explicitly calling [Rows.Close],
// calling [Rows.Next] until it returns false, or when a fatal error occurs.
//
// Once a Rows is closed the only methods that may be called are [Rows.Close], [Rows.Err],
// and [Rows.CommandTag].
//
// Rows is an interface instead of a struct to allow tests to mock Query. However,
// adding a method to an interface is technically a breaking change. Because of this
// the Rows interface is partially excluded from semantic version requirements.
// Methods will not be removed or changed, but new methods may be added.
type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close()

	// Err returns any error that occurred while executing a query or reading its results. Err must be called after the
	// Rows is closed (either by calling Close or by Next returning false) to check if the query was successful. If it is
	// called before the Rows is closed it may return nil even if the query failed on the server.
	Err() error

	// CommandTag returns the command tag from this query. It is only available after Rows is closed.
	CommandTag() pgconn.CommandTag

	// FieldDescriptions returns the field descriptions of the columns. It may return nil. In particular this can occur
	// when there was an error executing the query.
	FieldDescriptions() []pgconn.FieldDescription

	// Next prepares the next row for reading. It returns true if there is another row and false if no more rows are
	// available or a fatal error has occurred. It automatically closes rows upon returning false (whether due to all rows
	// having been read or due to an error).
	//
	// Callers should check rows.Err() after rows.Next() returns false to detect whether result-set reading ended
	// prematurely due to an error. See [Conn.Query] for details.
	//
	// For simpler error handling, consider using the higher-level pgx v5 [CollectRows()] and [ForEachRow()] helpers instead.
	Next() bool

	// Scan reads the values from the current row into dest values positionally. dest can include pointers to core types,
	// values implementing the Scanner interface, and nil. nil will skip the value entirely. It is an error to call Scan
	// without first calling Next() and checking that it returned true. Rows is automatically closed upon error.
	//
	// As a special case, if dest is a single value implementing [RowScanner], the whole row is given to its ScanRow
	// method instead of being scanned positionally.
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

	// TypeMap returns the [pgtype.Map] the values of this Rows are decoded with. It is available even when [Rows.Conn]
	// is nil, such as for a Rows created by [RowsFromResultReader]. It may return nil if the Rows carries no values,
	// such as one representing only an error.
	TypeMap() *pgtype.Map
}

// Row is a convenience wrapper over [Rows] that is returned by [Conn.QueryRow].
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

// RowScanner scans an entire row at a time into the RowScanner. It is only used when it is the sole destination passed
// to [Rows.Scan] or [Row.Scan]. When passed alongside other destinations it is scanned as an ordinary single value.
//
// ScanRow always takes precedence over the destination's other scanning interfaces, such as
// [pgtype.CompositeIndexScanner]. A type implementing both must therefore dispatch within ScanRow, because the number of
// columns is not known until the row arrives:
//
//	func (p *Person) ScanRow(rows pgx.Rows) error {
//		if fds := rows.FieldDescriptions(); len(fds) == 1 {
//			// Scan the single column via p's pgtype.CompositeIndexScanner implementation. rows.Scan(p) would
//			// call ScanRow again.
//			return rows.TypeMap().Scan(fds[0].DataTypeOID, fds[0].Format, rows.RawValues()[0], p)
//		}
//		return rows.Scan(&p.Name, &p.Age)
//	}
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
		rows.batchTracer.TraceBatchQuery(rows.ctx, rows.conn, TraceBatchQueryData{SQL: rows.sql, Args: rows.args, CommandTag: rows.commandTag, Err: rows.err, ReadStartTime: rows.startTime})
	} else if rows.queryTracer != nil {
		rows.queryTracer.TraceQueryEnd(rows.ctx, rows.conn, TraceQueryEndData{rows.commandTag, rows.err})
	}

	// Zero references to other memory allocations. This allows them to be GC'd even when the Rows still referenced. In
	// particular, when using pgxpool GC could be delayed as pgxpool.poolRows are allocated in large slices.
	//
	// https://github.com/jackc/pgx/pull/2269
	rows.values = nil
	rows.scanPlans = nil
	rows.scanTypes = nil
	rows.ctx = nil
	rows.sql = ""
	rows.args = nil
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
			err = ScanArgError{ColumnIndex: i, FieldName: fieldDescriptions[i].Name, Err: err}
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

	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) != len(rows.values) {
		err := fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(rows.values))
		rows.fatal(err)
		return nil, err
	}

	values := make([]any, 0, len(fieldDescriptions))

	for i := range fieldDescriptions {
		buf := rows.values[i]
		fd := &fieldDescriptions[i]

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

func (rows *baseRows) TypeMap() *pgtype.Map {
	return rows.typeMap
}

func (rows *baseRows) Conn() *Conn {
	return rows.conn
}

type ScanArgError struct {
	ColumnIndex int
	FieldName   string
	Err         error
}

func (e ScanArgError) Error() string {
	if e.FieldName == "?column?" { // Don't include the fieldname if it's unknown
		return fmt.Sprintf("can't scan into dest[%d]: %v", e.ColumnIndex, e.Err)
	}

	return fmt.Sprintf("can't scan into dest[%d] (col: %s): %v", e.ColumnIndex, e.FieldName, e.Err)
}

func (e ScanArgError) Unwrap() error {
	return e.Err
}

// ScanRow decodes raw row data into dest. It can be used to scan rows read from the lower level [pgconn] interface.
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
			return ScanArgError{ColumnIndex: i, FieldName: fieldDescriptions[i].Name, Err: err}
		}
	}

	return nil
}

// RowsFromResultReader returns a [Rows] that will read from values resultReader and decode with typeMap. It can be used
// to read from the lower level [pgconn] interface.
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

// AppendRows iterates through rows, calling fn for each row, and appending the results into a slice of T.
//
// This function closes the rows automatically on return.
func AppendRows[T any, S ~[]T](slice S, rows Rows, fn RowToFunc[T]) (S, error) {
	cr := collectRowsPool.Get().(*collectRows)
	cr.Rows = rows
	defer func() {
		clear(cr.targets[:cap(cr.targets)])
		cr.addrTarget[0] = nil
		if cr.planMap != nil {
			clear(cr.planMap)
		}
		*cr = collectRows{targets: cr.targets[:0], planMap: cr.planMap}
		collectRowsPool.Put(cr)
		rows.Close()
	}()

	for rows.Next() {
		value, err := fn(cr)
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

// collectRows wraps Rows during AppendRows to carry a cached RowPlan and a
// reusable Scan destination slice across the collection. Pooling collectRows
// avoids a wrapper allocation per AppendRows call and retains the targets
// backing array across queries.
type collectRows struct {
	Rows
	targets        []any
	targetsClaimed bool
	// addrTarget provides dedicated, non-aliasing destination storage for
	// RowToAddrOf, preventing it from clobbering targets used by cached plans.
	addrTarget [1]any
	planType   reflect.Type
	planVal    any
	planMap    map[reflect.Type]any
}

var collectRowsPool = sync.Pool{
	New: func() any { return &collectRows{} },
}

// claimTargets loans the pooled targets slice to the first plan in the
// collection. Subsequent plans allocate dedicated slices so they never alias.
func (cr *collectRows) claimTargets(n int) []any {
	if !cr.targetsClaimed {
		cr.targetsClaimed = true
		if cap(cr.targets) < n {
			cr.targets = make([]any, n)
		} else {
			cr.targets = cr.targets[:n]
		}
		return cr.targets
	}
	return make([]any, n)
}

// RowPlan retrieves or initializes a collection-scoped plan P for row.
//
// When row is passed to a [RowToFunc] by [CollectRows] or [AppendRows], init is called once on the first row for plan
// type P and the returned *P is cached and reused for subsequent rows in that collection. Outside [CollectRows] and
// [AppendRows] (such as in [CollectOneRow] or a manual [Rows.Next] loop), init is called on each invocation.
//
// This allows custom [RowToFunc] implementations to amortize per-query reflection or column mapping work across a
// result set without changing function signatures or managing stateful closures.
func RowPlan[P any](row CollectableRow, init func(row CollectableRow) (*P, error)) (*P, error) {
	cr, ok := row.(*collectRows)
	if !ok {
		return init(row)
	}

	pType := reflect.TypeFor[P]()
	if cr.planType == pType {
		return cr.planVal.(*P), nil
	}
	if cr.planMap != nil {
		if v, ok := cr.planMap[pType]; ok {
			return v.(*P), nil
		}
	}

	plan, err := init(row)
	if err != nil {
		return nil, err
	}

	if cr.planType == nil {
		cr.planType = pType
		cr.planVal = plan
	} else {
		if cr.planMap == nil {
			cr.planMap = make(map[reflect.Type]any, 2)
		}
		cr.planMap[pType] = plan
	}
	return plan, nil
}

// CollectRows iterates through rows, calling fn for each row, and collecting the results into a slice of T.
//
// This function closes the rows automatically on return.
func CollectRows[T any](rows Rows, fn RowToFunc[T]) ([]T, error) {
	return AppendRows([]T{}, rows, fn)
}

// CollectRowsInto is the same as [CollectRows] but allows
// defining a custom slice type. Useful when you have custom
// types to denote slices.
//
// This function closes the rows automatically on return.
func CollectRowsInto[S ~[]T, T any](rows Rows, fn RowToFunc[T]) (S, error) {
	return CollectRows(rows, fn)
}

// CollectOneRow calls fn for the first row in rows and returns the result. If no rows are found returns an error where errors.Is(ErrNoRows) is true.
// CollectOneRow is to [CollectRows] as [Conn.QueryRow] is to [Conn.Query].
//
// This function closes the rows automatically on return.
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

	// The defer rows.Close() won't have executed yet. If the query returned more than one row, rows would still be open.
	// rows.Close() must be called before rows.Err() so we explicitly call it here.
	rows.Close()
	return value, rows.Err()
}

// CollectExactlyOneRow calls fn for the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
//
// This function closes the rows automatically on return.
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
	cr, ok := row.(*collectRows)
	if !ok {
		var value T
		err := row.Scan(&value)
		return value, err
	}
	plan, err := RowPlan(cr, func(r CollectableRow) (*structScanPlan[T, modeScalar], error) {
		p := &structScanPlan[T, modeScalar]{targets: r.(*collectRows).claimTargets(1)}
		if canReuseScratchType(reflect.TypeFor[T]()) {
			p.targets[0] = &p.dst
		} else {
			p.perRow = true
		}
		return p, nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return plan.scan(cr)
}

// RowToAddrOf returns the address of a T scanned from row.
func RowToAddrOf[T any](row CollectableRow) (*T, error) {
	var value T
	if cr, ok := row.(*collectRows); ok {
		// Use cr.addrTarget so RowToAddrOf does not mutate or alias cr.targets,
		// which may be shared by a cached structScanPlan in the same collection.
		cr.addrTarget[0] = &value
		err := cr.Rows.Scan(cr.addrTarget[:]...)
		return &value, err
	}
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
		(*rs)[rows.FieldDescriptions()[i].Name] = values[i]
	}

	return nil
}

func planPositionalStruct[T any](cr *collectRows) (*structScanPlan[T, modePos], error) {
	plan, err := RowPlan(cr, func(r CollectableRow) (*structScanPlan[T, modePos], error) {
		psf := lookupStructFields(reflect.TypeFor[T]())
		return newStructScanPlan[T, modePos](r.(*collectRows), psf.fields, psf.perRow), nil
	})
	if err != nil {
		return nil, err
	}
	if rawLen := len(cr.RawValues()); rawLen > len(plan.targets) {
		return nil, fmt.Errorf(
			"got %d values, but dst struct has only %d fields",
			rawLen,
			len(plan.targets),
		)
	}
	return plan, nil
}

// RowToStructByPos returns a T scanned from row. T must be a struct. T must have the same number of public fields as row
// has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then the field will be
// ignored.
func RowToStructByPos[T any](row CollectableRow) (T, error) {
	cr, ok := row.(*collectRows)
	if !ok {
		ptr, err := scanPositionalStructOneRowAddr[T](row)
		return *ptr, err
	}
	plan, err := planPositionalStruct[T](cr)
	if err != nil {
		var zero T
		return zero, err
	}
	return plan.scan(cr)
}

func scanPositionalStructOneRowAddr[T any](row CollectableRow) (*T, error) {
	psf := lookupStructFields(reflect.TypeFor[T]())
	if rawLen := len(row.RawValues()); rawLen > len(psf.fields) {
		var zero T
		return &zero, fmt.Errorf(
			"got %d values, but dst struct has only %d fields",
			rawLen,
			len(psf.fields),
		)
	}
	var dst T
	targets := make([]any, len(psf.fields))
	bindStructScanTargets(&dst, psf.fields, targets)
	err := row.Scan(targets...)
	return &dst, err
}

// RowToAddrOfStructByPos returns the address of a T scanned from row. T must be a struct. T must have the same number a
// public fields as row has fields. The row and T fields will be matched by position. If the "db" struct tag is "-" then
// the field will be ignored.
func RowToAddrOfStructByPos[T any](row CollectableRow) (*T, error) {
	cr, ok := row.(*collectRows)
	if !ok {
		return scanPositionalStructOneRowAddr[T](row)
	}
	plan, err := planPositionalStruct[T](cr)
	if err != nil {
		var zero T
		return &zero, err
	}
	return plan.scanAddr(cr)
}

type (
	modeScalar  struct{}
	modePos     struct{}
	modeName    struct{}
	modeNameLax struct{}
)

// structScanPlan pre-binds Scan target pointers to a reusable dst struct once
// on the first row. Zeroing dst in place after each Scan and returning a value
// copy eliminates per-row reflect.Value.Field(...).Addr().Interface() calls,
// per-row joinFieldNames/sync.Map lookups, and (for value scanners) the per-row
// heap allocation caused by taking the address of a local T.
type structScanPlan[T, Mode any] struct {
	dst     T
	targets []any
	dirty   bool
	perRow  bool
}

func newStructScanPlan[T, Mode any](cr *collectRows, fields []structRowField, perRow bool) *structScanPlan[T, Mode] {
	if perRow {
		targets := cr.claimTargets(len(fields) + 1)
		targets[len(fields)] = fields
		return &structScanPlan[T, Mode]{
			targets: targets[:len(fields)],
			perRow:  true,
		}
	}
	p := &structScanPlan[T, Mode]{targets: cr.claimTargets(len(fields))}
	bindStructScanTargets(&p.dst, fields, p.targets)
	return p
}

func bindStructScanTargets[T any](dst *T, fields []structRowField, targets []any) {
	v := reflect.ValueOf(dst).Elem()
	for i, f := range fields {
		// v.Field avoids FieldByIndex's slice iteration for top-level fields.
		if len(f.path) == 1 {
			targets[i] = v.Field(f.path[0]).Addr().Interface()
		} else {
			targets[i] = v.FieldByIndex(f.path).Addr().Interface()
		}
	}
}

func (p *structScanPlan[T, Mode]) scan(cr *collectRows) (T, error) {
	if p.perRow {
		dst, err := p.scanPerRowAddr(cr)
		return *dst, err
	}
	if p.dirty {
		var zero T
		p.dst = zero
	}
	p.dirty = true
	err := cr.Rows.Scan(p.targets...)
	return p.dst, err
}

func (p *structScanPlan[T, Mode]) scanAddr(cr *collectRows) (*T, error) {
	if p.perRow {
		return p.scanPerRowAddr(cr)
	}
	if p.dirty {
		var zero T
		p.dst = zero
	}
	p.dirty = true
	err := cr.Rows.Scan(p.targets...)
	dst := p.dst
	return &dst, err
}

// scanPerRowAddr scans into an independently allocated dst on each row instead
// of the shared p.dst scratch struct. Returning *T directly ensures both that
// custom scanners retaining interior references (e.g. s.val = s.buf[:]) receive
// isolated memory per row and that pointer-returning helpers (RowToAddrOfStruct*)
// return the exact struct instance that was scanned into.
func (p *structScanPlan[T, Mode]) scanPerRowAddr(cr *collectRows) (*T, error) {
	var dst T
	if reflect.TypeFor[Mode]() == reflect.TypeFor[modeScalar]() {
		p.targets[0] = &dst
	} else {
		// Retrieve the cached []structRowField stored in the extra capacity slot
		// of p.targets by newStructScanPlan.
		fields := p.targets[:len(p.targets)+1][len(p.targets)].([]structRowField)
		bindStructScanTargets(&dst, fields, p.targets)
	}
	err := cr.Rows.Scan(p.targets[:len(p.targets):len(p.targets)]...)
	return &dst, err
}

// canReuseScratchType reports whether scanning into a reusable scratch T and
// returning a shallow value copy is guaranteed not to retain pointers or slice
// headers referencing T's own memory across rows.
//
// Custom scanner types (any non-stdlib, non-pgtype type with methods on *T) and
// composite types containing pointer-bearing fields may retain references to
// their scan receiver (e.g. `s.val = s.buf[:]`, `s.ptr = &s.val`, or a named
// slice storing `s`), so they use per-row allocation instead of scratch reuse.
func canReuseScratchType(t reflect.Type) bool {
	return canReuseScratchTypeDepth(t, 0)
}

func canReuseScratchTypeDepth(t reflect.Type, depth int) bool {
	if depth > 8 {
		return false
	}
	if pkg := t.PkgPath(); pkg != "" {
		switch pkg {
		case "time":
			return t.Name() == "Time" || t.Name() == "Duration"
		case "net/netip":
			return true
		case "database/sql":
			if strings.HasPrefix(t.Name(), "Null[") {
				return canReuseScratchTypeDepth(t.Field(0).Type, depth+1)
			}
			return t.Kind() == reflect.Struct
		case "github.com/jackc/pgx/v5/pgtype":
			if strings.HasPrefix(t.Name(), "Range[") {
				return canReuseScratchTypeDepth(t.Field(0).Type, depth+1)
			}
			if strings.HasPrefix(t.Name(), "Array[") {
				return canReuseScratchTypeDepth(t.Field(0).Type.Elem(), depth+1)
			}
			if strings.HasPrefix(t.Name(), "FlatArray[") || strings.HasPrefix(t.Name(), "Multirange[") {
				return canReuseScratchTypeDepth(t.Elem(), depth+1)
			}
			return t.Kind() == reflect.Struct
		}
		if reflect.PointerTo(t).NumMethod() > 0 {
			return false
		}
	}

	switch t.Kind() {
	case reflect.Struct:
		return !typeHasPointers(t)
	case reflect.Array:
		return t.Len() == 0 || !typeHasPointers(t.Elem())
	case reflect.Slice, reflect.Pointer:
		// Only unnamed slices and pointers (t.PkgPath() == "") cannot define
		// custom scanner methods or form direct recursive slice/pointer cycles.
		return t.PkgPath() == "" && canReuseScratchTypeDepth(t.Elem(), depth+1)
	case reflect.Map, reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		return false
	default:
		return true
	}
}

func typeHasPointers(t reflect.Type) bool {
	if pkg := t.PkgPath(); pkg != "" {
		if (pkg == "time" && (t.Name() == "Time" || t.Name() == "Duration")) ||
			pkg == "net/netip" {
			return false
		}
		if pkg != "database/sql" &&
			pkg != "github.com/jackc/pgx/v5/pgtype" &&
			reflect.PointerTo(t).NumMethod() > 0 {
			return true
		}
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Pointer, reflect.UnsafePointer, reflect.Interface, reflect.Map, reflect.Chan, reflect.Func:
		return true
	case reflect.Array:
		return t.Len() > 0 && typeHasPointers(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeHasPointers(t.Field(i).Type) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func fieldsNeedPerRowAlloc(t reflect.Type, fields []structRowField) bool {
	for _, f := range fields {
		if f.path != nil && !canReuseScratchType(t.FieldByIndex(f.path).Type) {
			return true
		}
	}
	return false
}

type positionalStructFields struct {
	fields []structRowField
	perRow bool
}

// Map from reflect.Type -> *positionalStructFields
var positionalStructFieldMap sync.Map

func lookupStructFields(t reflect.Type) *positionalStructFields {
	if cached, ok := positionalStructFieldMap.Load(t); ok {
		return cached.(*positionalStructFields)
	}

	fieldStack := make([]int, 0, 1)
	fields := computeStructFields(t, make([]structRowField, 0, t.NumField()), &fieldStack)
	psf := &positionalStructFields{
		fields: fields,
		perRow: fieldsNeedPerRowAlloc(t, fields),
	}
	fieldsIface, _ := positionalStructFieldMap.LoadOrStore(t, psf)
	return fieldsIface.(*positionalStructFields)
}

func computeStructFields(
	t reflect.Type,
	fields []structRowField,
	fieldStack *[]int,
) []structRowField {
	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			fields = computeStructFields(sf.Type, fields, fieldStack)
		} else if sf.PkgPath == "" {
			dbTag, _ := sf.Tag.Lookup(structTagKey)
			if dbTag == "-" {
				// Field is ignored, skip it.
				continue
			}
			fields = append(fields, structRowField{
				path: append([]int(nil), *fieldStack...),
			})
		}
	}
	*fieldStack = (*fieldStack)[:tail]
	return fields
}

// RowToStructByName returns a T scanned from row. T must be a struct. T must have the same number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByName[T any](row CollectableRow) (T, error) {
	return scanNamedStruct[T, modeName](row)
}

// RowToAddrOfStructByName returns the address of a T scanned from row. T must be a struct. T must have the same number
// of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByName[T any](row CollectableRow) (*T, error) {
	return scanNamedStructAddr[T, modeName](row)
}

// RowToStructByNameLax returns a T scanned from row. T must be a struct. T must have greater than or equal number of named public
// fields as row has fields. The row and T fields will be matched by name. The match is case-insensitive. The database
// column name can be overridden with a "db" struct tag. If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByNameLax[T any](row CollectableRow) (T, error) {
	return scanNamedStruct[T, modeNameLax](row)
}

// RowToAddrOfStructByNameLax returns the address of a T scanned from row. T must be a struct. T must have greater than or
// equal number of named public fields as row has fields. The row and T fields will be matched by name. The match is
// case-insensitive. The database column name can be overridden with a "db" struct tag. If the "db" struct tag is "-"
// then the field will be ignored.
func RowToAddrOfStructByNameLax[T any](row CollectableRow) (*T, error) {
	return scanNamedStructAddr[T, modeNameLax](row)
}

func planNamedStruct[T, Mode any](cr *collectRows) (*structScanPlan[T, Mode], error) {
	return RowPlan(cr, func(r CollectableRow) (*structScanPlan[T, Mode], error) {
		nsf, err := lookupNamedStructFields(reflect.TypeFor[T](), r.FieldDescriptions())
		if err != nil {
			return nil, err
		}
		if reflect.TypeFor[Mode]() != reflect.TypeFor[modeNameLax]() && nsf.missingField != "" {
			return nil, fmt.Errorf("cannot find field %s in returned row", nsf.missingField)
		}
		return newStructScanPlan[T, Mode](r.(*collectRows), nsf.fields, nsf.perRow), nil
	})
}

func scanNamedStruct[T, Mode any](row CollectableRow) (T, error) {
	cr, ok := row.(*collectRows)
	if !ok {
		ptr, err := scanNamedStructOneRowAddr[T, Mode](row)
		return *ptr, err
	}
	plan, err := planNamedStruct[T, Mode](cr)
	if err != nil {
		var zero T
		return zero, err
	}
	return plan.scan(cr)
}

func scanNamedStructAddr[T, Mode any](row CollectableRow) (*T, error) {
	cr, ok := row.(*collectRows)
	if !ok {
		return scanNamedStructOneRowAddr[T, Mode](row)
	}
	plan, err := planNamedStruct[T, Mode](cr)
	if err != nil {
		var zero T
		return &zero, err
	}
	return plan.scanAddr(cr)
}

func scanNamedStructOneRowAddr[T, Mode any](row CollectableRow) (*T, error) {
	var dst T
	nsf, err := lookupNamedStructFields(reflect.TypeFor[T](), row.FieldDescriptions())
	if err != nil {
		return &dst, err
	}
	if reflect.TypeFor[Mode]() != reflect.TypeFor[modeNameLax]() && nsf.missingField != "" {
		return &dst, fmt.Errorf("cannot find field %s in returned row", nsf.missingField)
	}
	targets := make([]any, len(nsf.fields))
	bindStructScanTargets(&dst, nsf.fields, targets)
	err = row.Scan(targets...)
	return &dst, err
}

// Map from namedStructFieldMap -> *namedStructFields
var namedStructFieldMap sync.Map

type namedStructFieldsKey struct {
	t        reflect.Type
	colNames string
}

type namedStructFields struct {
	fields []structRowField
	// missingField is the first field from the struct without a corresponding row field.
	// This is used to construct the correct error message for non-lax queries.
	missingField string
	perRow       bool
}

func lookupNamedStructFields(
	t reflect.Type,
	fldDescs []pgconn.FieldDescription,
) (*namedStructFields, error) {
	key := namedStructFieldsKey{
		t:        t,
		colNames: joinFieldNames(fldDescs),
	}
	if cached, ok := namedStructFieldMap.Load(key); ok {
		return cached.(*namedStructFields), nil
	}

	// We could probably do two-levels of caching, where we compute the key -> fields mapping
	// for a type only once, cache it by type, then use that to compute the column -> fields
	// mapping for a given set of columns.
	fieldStack := make([]int, 0, 1)
	fields, missingField := computeNamedStructFields(
		fldDescs,
		t,
		make([]structRowField, len(fldDescs)),
		&fieldStack,
	)
	for i, f := range fields {
		if f.path == nil {
			return nil, fmt.Errorf(
				"struct doesn't have corresponding row field %s",
				fldDescs[i].Name,
			)
		}
	}

	fieldsIface, _ := namedStructFieldMap.LoadOrStore(
		key,
		&namedStructFields{
			fields:       fields,
			missingField: missingField,
			perRow:       fieldsNeedPerRowAlloc(t, fields),
		},
	)
	return fieldsIface.(*namedStructFields), nil
}

func joinFieldNames(fldDescs []pgconn.FieldDescription) string {
	switch len(fldDescs) {
	case 0:
		return ""
	case 1:
		return fldDescs[0].Name
	}

	totalSize := len(fldDescs) - 1 // Space for separator bytes.
	for _, d := range fldDescs {
		totalSize += len(d.Name)
	}
	var b strings.Builder
	b.Grow(totalSize)
	b.WriteString(fldDescs[0].Name)
	for _, d := range fldDescs[1:] {
		b.WriteByte(0) // Join with NUL byte as it's (presumably) not a valid column character.
		b.WriteString(d.Name)
	}
	return b.String()
}

func computeNamedStructFields(
	fldDescs []pgconn.FieldDescription,
	t reflect.Type,
	fields []structRowField,
	fieldStack *[]int,
) ([]structRowField, string) {
	var missingField string
	tail := len(*fieldStack)
	*fieldStack = append(*fieldStack, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		(*fieldStack)[tail] = i
		if sf.PkgPath != "" && !sf.Anonymous {
			// Field is unexported, skip it.
			continue
		}
		// Handle anonymous struct embedding, but do not try to handle embedded pointers.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			var missingSubField string
			fields, missingSubField = computeNamedStructFields(
				fldDescs,
				sf.Type,
				fields,
				fieldStack,
			)
			if missingField == "" {
				missingField = missingSubField
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
			fpos := fieldPosByName(fldDescs, colName, !dbTagPresent)
			if fpos == -1 {
				if missingField == "" {
					missingField = colName
				}
				continue
			}
			fields[fpos] = structRowField{
				path: append([]int(nil), *fieldStack...),
			}
		}
	}
	*fieldStack = (*fieldStack)[:tail]

	return fields, missingField
}

const structTagKey = "db"

func fieldPosByName(fldDescs []pgconn.FieldDescription, field string, normalize bool) (i int) {
	i = -1

	if normalize {
		field = strings.ReplaceAll(field, "_", "")
	} else {
		// Explicit db tags can distinguish quoted identifiers that differ only by case.
		for i, desc := range fldDescs {
			if desc.Name == field {
				return i
			}
		}
	}
	for i, desc := range fldDescs {
		if normalize {
			if strings.EqualFold(strings.ReplaceAll(desc.Name, "_", ""), field) {
				return i
			}
		} else {
			if strings.EqualFold(desc.Name, field) {
				return i
			}
		}
	}
	return i
}

// structRowField describes a field of a struct.
//
// TODO: It would be a bit more efficient to track the path using the pointer
// offset within the (outermost) struct and use unsafe.Pointer arithmetic to
// construct references when scanning rows. However, it's not clear it's worth
// using unsafe for this.
type structRowField struct {
	path []int
}
