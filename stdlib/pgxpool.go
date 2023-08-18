package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OptionOpenDBFromPool options for configuring the driver connector when
// opening a new *sql.DB from *pgxpool.Pool.
type OptionOpenDBFromPool func(*poolConnector)

// OptionBeforeConnect provides a callback for before acquire.
func OptionBeforeAcquire(f func(context.Context, *pgxpool.Pool) error) OptionOpenDBFromPool {
	return func(c *poolConnector) {
		c.BeforeAcquire = f
	}
}

// OptionAfterAcquire provides a callback for after acquire.
func OptionAfterAcquire(f func(context.Context, *pgxpool.Conn) error) OptionOpenDBFromPool {
	return func(c *poolConnector) {
		c.AfterAcquire = f
	}
}

// OptionResetSession provides a callback that can be used to add custom logic
// prior to executing a query on the connection if the connection has been used
// before. If ResetSessionFunc returns ErrBadConn error the connection will be
// discarded.
func OptionPoolConnResetSession(f func(context.Context, *pgxpool.Conn) error) OptionOpenDBFromPool {
	return func(c *poolConnector) {
		c.ResetSession = f
	}
}

// GetPoolConnector creates a new driver connector to open a new *sql.DB from
// the provided pgx pool.
func GetPoolConnector(pool *pgxpool.Pool, opts ...OptionOpenDBFromPool) driver.Connector {
	c := &poolConnector{pool: pool, driver: pgxDriver}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// OpenDBFromPool opens a database using the provided pgx pool. It sets the
// maximum number of connections in the idle connection pool to zero, since
// those connections are managed in this pgx pool.
func OpenDBFromPool(pool *pgxpool.Pool, opts ...OptionOpenDBFromPool) *sql.DB {
	c := GetPoolConnector(pool, opts...)
	db := sql.OpenDB(c)
	db.SetMaxIdleConns(0)
	return db
}

type poolConnector struct {
	pool          *pgxpool.Pool
	BeforeAcquire func(context.Context, *pgxpool.Pool) error // function to call before acquiring of every new connection
	AfterAcquire  func(context.Context, *pgxpool.Conn) error // function to call after acquiring of every new connection
	ResetSession  func(context.Context, *pgxpool.Conn) error // function is called before a connection is reused
	driver        driver.Driver
}

func (c *poolConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := c.BeforeAcquire(ctx, c.pool); err != nil {
		return nil, err
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.AfterAcquire(ctx, conn); err != nil {
		return nil, err
	}

	return &PoolConn{conn: conn}, nil
}

func (c *poolConnector) Driver() driver.Driver {
	return c.driver
}

type PoolConn struct {
	conn                 *pgxpool.Conn
	psCount              int64                                  // Counter used for creating unique prepared statement names
	resetSessionFunc     func(context.Context, *pgx.Conn) error // Function is called before a connection is reused
	lastResetSessionTime time.Time
}

// Conn returns the underlying *pgx.Conn
func (c *PoolConn) Conn() *pgx.Conn {
	return c.conn.Conn()
}

func (c *PoolConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *PoolConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	conn := c.Conn()

	if conn.IsClosed() {
		return nil, driver.ErrBadConn
	}

	name := fmt.Sprintf("pgx_%d", c.psCount)
	c.psCount++

	sd, err := conn.Prepare(ctx, name, query)
	if err != nil {
		return nil, err
	}

	return &PoolStmt{sd: sd, conn: c}, nil
}

func (c *PoolConn) Close() error {
	c.conn.Release()
	return nil
}

func (c *PoolConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *PoolConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.Conn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	var pgxOpts pgx.TxOptions
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	case sql.LevelReadUncommitted:
		pgxOpts.IsoLevel = pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		pgxOpts.IsoLevel = pgx.ReadCommitted
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		pgxOpts.IsoLevel = pgx.RepeatableRead
	case sql.LevelSerializable:
		pgxOpts.IsoLevel = pgx.Serializable
	default:
		return nil, fmt.Errorf("unsupported isolation: %v", opts.Isolation)
	}

	if opts.ReadOnly {
		pgxOpts.AccessMode = pgx.ReadOnly
	}

	tx, err := c.conn.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	return wrapTx{ctx: ctx, tx: tx}, nil
}

func (c *PoolConn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	if c.Conn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := namedValueToInterface(argsV)

	commandTag, err := c.conn.Exec(ctx, query, args...)
	// if we got a network error before we had a chance to send the query, retry
	if err != nil {
		if pgconn.SafeToRetry(err) {
			return nil, driver.ErrBadConn
		}
	}
	return driver.RowsAffected(commandTag.RowsAffected()), err
}

func (c *PoolConn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if c.Conn().IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := []any{databaseSQLResultFormats}
	args = append(args, namedValueToInterface(argsV)...)

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		if pgconn.SafeToRetry(err) {
			return nil, driver.ErrBadConn
		}
		return nil, err
	}

	// Preload first row because otherwise we won't know what columns are available when database/sql asks.
	more := rows.Next()
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return &PoolRows{conn: c, rows: rows, skipNext: true, skipNextMore: more}, nil
}

func (c *PoolConn) Ping(ctx context.Context) error {
	if c.Conn().IsClosed() {
		return driver.ErrBadConn
	}

	err := c.conn.Ping(ctx)
	if err != nil {
		// A Ping failure implies some sort of fatal state. The connection is almost certainly already closed by the
		// failure, but manually close it just to be sure.
		c.Close()
		return driver.ErrBadConn
	}

	return nil
}

func (c *PoolConn) CheckNamedValue(*driver.NamedValue) error {
	// Underlying pgx supports sql.Scanner and driver.Valuer interfaces natively. So everything can be passed through directly.
	return nil
}

func (c *PoolConn) ResetSession(ctx context.Context) error {
	if c.Conn().IsClosed() {
		return driver.ErrBadConn
	}

	now := time.Now()
	if now.Sub(c.lastResetSessionTime) > time.Second {
		if err := c.Conn().PgConn().CheckConn(); err != nil {
			return driver.ErrBadConn
		}
	}
	c.lastResetSessionTime = now

	return c.resetSessionFunc(ctx, c.Conn())
}

type PoolStmt struct {
	sd   *pgconn.StatementDescription
	conn *PoolConn
}

func (s *PoolStmt) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return s.conn.Conn().Deallocate(ctx, s.sd.Name)
}

func (s *PoolStmt) NumInput() int {
	return len(s.sd.ParamOIDs)
}

func (s *PoolStmt) Exec(argsV []driver.Value) (driver.Result, error) {
	return nil, errors.New("Stmt.Exec deprecated and not implemented")
}

func (s *PoolStmt) ExecContext(ctx context.Context, argsV []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.sd.Name, argsV)
}

func (s *PoolStmt) Query(argsV []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Stmt.Query deprecated and not implemented")
}

func (s *PoolStmt) QueryContext(ctx context.Context, argsV []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.sd.Name, argsV)
}

type PoolRows struct {
	conn         *PoolConn
	rows         pgx.Rows
	valueFuncs   []rowValueFunc
	skipNext     bool
	skipNextMore bool

	columnNames []string
}

func (r *PoolRows) Columns() []string {
	if r.columnNames == nil {
		fields := r.rows.FieldDescriptions()
		r.columnNames = make([]string, len(fields))
		for i, fd := range fields {
			r.columnNames[i] = string(fd.Name)
		}
	}

	return r.columnNames
}

// ColumnTypeDatabaseTypeName returns the database system type name. If the name is unknown the OID is returned.
func (r *PoolRows) ColumnTypeDatabaseTypeName(index int) string {
	if dt, ok := r.conn.Conn().TypeMap().TypeForOID(r.rows.FieldDescriptions()[index].DataTypeOID); ok {
		return strings.ToUpper(dt.Name)
	}

	return strconv.FormatInt(int64(r.rows.FieldDescriptions()[index].DataTypeOID), 10)
}

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (r *PoolRows) ColumnTypeLength(index int) (int64, bool) {
	fd := r.rows.FieldDescriptions()[index]

	switch fd.DataTypeOID {
	case pgtype.TextOID, pgtype.ByteaOID:
		return math.MaxInt64, true
	case pgtype.VarcharOID, pgtype.BPCharArrayOID:
		return int64(fd.TypeModifier - varHeaderSize), true
	default:
		return 0, false
	}
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, ok should be false.
func (r *PoolRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	fd := r.rows.FieldDescriptions()[index]

	switch fd.DataTypeOID {
	case pgtype.NumericOID:
		mod := fd.TypeModifier - varHeaderSize
		precision = int64((mod >> 16) & 0xffff)
		scale = int64(mod & 0xffff)
		return precision, scale, true
	default:
		return 0, 0, false
	}
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (r *PoolRows) ColumnTypeScanType(index int) reflect.Type {
	fd := r.rows.FieldDescriptions()[index]

	switch fd.DataTypeOID {
	case pgtype.Float8OID:
		return reflect.TypeOf(float64(0))
	case pgtype.Float4OID:
		return reflect.TypeOf(float32(0))
	case pgtype.Int8OID:
		return reflect.TypeOf(int64(0))
	case pgtype.Int4OID:
		return reflect.TypeOf(int32(0))
	case pgtype.Int2OID:
		return reflect.TypeOf(int16(0))
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.NumericOID:
		return reflect.TypeOf(float64(0))
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return reflect.TypeOf(time.Time{})
	case pgtype.ByteaOID:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf("")
	}
}

func (r *PoolRows) Close() error {
	r.rows.Close()
	return r.rows.Err()
}

func (r *PoolRows) Next(dest []driver.Value) error {
	m := r.conn.Conn().TypeMap()
	fieldDescriptions := r.rows.FieldDescriptions()

	if r.valueFuncs == nil {
		r.valueFuncs = make([]rowValueFunc, len(fieldDescriptions))

		for i, fd := range fieldDescriptions {
			dataTypeOID := fd.DataTypeOID
			format := fd.Format

			switch fd.DataTypeOID {
			case pgtype.BoolOID:
				var d bool
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return d, err
				}
			case pgtype.ByteaOID:
				var d []byte
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return d, err
				}
			case pgtype.CIDOID, pgtype.OIDOID, pgtype.XIDOID:
				var d pgtype.Uint32
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.DateOID:
				var d pgtype.Date
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.Float4OID:
				var d float32
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return float64(d), err
				}
			case pgtype.Float8OID:
				var d float64
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return d, err
				}
			case pgtype.Int2OID:
				var d int16
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return int64(d), err
				}
			case pgtype.Int4OID:
				var d int32
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return int64(d), err
				}
			case pgtype.Int8OID:
				var d int64
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return d, err
				}
			case pgtype.JSONOID, pgtype.JSONBOID:
				var d []byte
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					if err != nil {
						return nil, err
					}
					return d, nil
				}
			case pgtype.TimestampOID:
				var d pgtype.Timestamp
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			case pgtype.TimestamptzOID:
				var d pgtype.Timestamptz
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					if err != nil {
						return nil, err
					}
					return d.Value()
				}
			default:
				var d string
				scanPlan := m.PlanScan(dataTypeOID, format, &d)
				r.valueFuncs[i] = func(src []byte) (driver.Value, error) {
					err := scanPlan.Scan(src, &d)
					return d, err
				}
			}
		}
	}

	var more bool
	if r.skipNext {
		more = r.skipNextMore
		r.skipNext = false
	} else {
		more = r.rows.Next()
	}

	if !more {
		if r.rows.Err() == nil {
			return io.EOF
		} else {
			return r.rows.Err()
		}
	}

	for i, rv := range r.rows.RawValues() {
		if rv != nil {
			var err error
			dest[i], err = r.valueFuncs[i](rv)
			if err != nil {
				return fmt.Errorf("convert field %d failed: %v", i, err)
			}
		} else {
			dest[i] = nil
		}
	}

	return nil
}
