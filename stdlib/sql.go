// Package stdlib is the compatibility layer from pgx to database/sql.
//
// A database/sql connection can be established through sql.Open.
//
//	db, err := sql.Open("pgx", "postgres://pgx_md5:secret@localhost:5432/pgx_test?sslmode=disable")
//	if err != nil {
//		return err
//	}
//
// Or from a DSN string.
//
//	db, err := sql.Open("pgx", "user=postgres password=secret host=localhost port=5432 database=pgx_test sslmode=disable")
//	if err != nil {
//		return err
//	}
//
// Or a pgx.ConnConfig can be used to set configuration not accessible via connection string. In this case the
// pgx.ConnConfig must first be registered with the driver. This registration returns a connection string which is used
// with sql.Open.
//
//	connConfig, _ := pgx.ParseConfig(os.Getenv("DATABASE_URL"))
//	connConfig.Logger = myLogger
//	connStr := stdlib.RegisterConnConfig(connConfig)
//	db, _ := sql.Open("pgx", connStr)
//
// pgx uses standard PostgreSQL positional parameters in queries. e.g. $1, $2.
// It does not support named parameters.
//
//	db.QueryRow("select * from users where id=$1", userID)
//
// AcquireConn and ReleaseConn acquire and release a *pgx.Conn from the standard
// database/sql.DB connection pool. This allows operations that must be
// performed on a single connection without running in a transaction, and it
// supports operations that use pgx specific functionality.
//
//	conn, err := stdlib.AcquireConn(db)
//	if err != nil {
//		return err
//	}
//	defer stdlib.ReleaseConn(db, conn)
//
//	// do stuff with pgx.Conn
//
// It also can be used to enable a fast path for pgx while preserving
// compatibility with other drivers and database.
//
//	conn, err := stdlib.AcquireConn(db)
//	if err == nil {
//		// fast path with pgx
//		// ...
//		// release conn when done
//		stdlib.ReleaseConn(db, conn)
//	} else {
//		// normal path for other drivers and databases
//	}
package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// Only intrinsic types should be binary format with database/sql.
var databaseSQLResultFormats pgx.QueryResultFormatsByOID

var pgxDriver *Driver

type ctxKey int

var ctxKeyFakeTx ctxKey = 0

var ErrNotPgx = errors.New("not pgx *sql.DB")

func init() {
	pgxDriver = &Driver{
		configs: make(map[string]*pgx.ConnConfig),
	}
	fakeTxConns = make(map[*pgx.Conn]*sql.Tx)
	sql.Register("pgx", pgxDriver)

	databaseSQLResultFormats = pgx.QueryResultFormatsByOID{
		pgtype.BoolOID:        1,
		pgtype.ByteaOID:       1,
		pgtype.CIDOID:         1,
		pgtype.DateOID:        1,
		pgtype.Float4OID:      1,
		pgtype.Float8OID:      1,
		pgtype.Int2OID:        1,
		pgtype.Int4OID:        1,
		pgtype.Int8OID:        1,
		pgtype.OIDOID:         1,
		pgtype.TimestampOID:   1,
		pgtype.TimestamptzOID: 1,
		pgtype.XIDOID:         1,
	}
}

var (
	fakeTxMutex sync.Mutex
	fakeTxConns map[*pgx.Conn]*sql.Tx
)

// GetDefaultDriver returns the driver initialized in the init function
// and used when the pgx driver is registered.
func GetDefaultDriver() driver.Driver {
	return pgxDriver
}

type Driver struct {
	configMutex sync.Mutex
	configs     map[string]*pgx.ConnConfig
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Ensure eventual timeout
	defer cancel()

	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &driverConnector{driver: d, name: name}, nil
}

func (d *Driver) registerConnConfig(c *pgx.ConnConfig) string {
	d.configMutex.Lock()
	connStr := fmt.Sprintf("registeredConnConfig%d", len(d.configs))
	d.configs[connStr] = c
	d.configMutex.Unlock()
	return connStr
}

func (d *Driver) unregisterConnConfig(connStr string) {
	d.configMutex.Lock()
	delete(d.configs, connStr)
	d.configMutex.Unlock()
}

type driverConnector struct {
	driver *Driver
	name   string
}

func (dc *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	var connConfig *pgx.ConnConfig

	dc.driver.configMutex.Lock()
	connConfig = dc.driver.configs[dc.name]
	dc.driver.configMutex.Unlock()

	if connConfig == nil {
		var err error
		connConfig, err = pgx.ParseConfig(dc.name)
		if err != nil {
			return nil, err
		}
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	c := &Conn{conn: conn, driver: dc.driver, connConfig: *connConfig}
	return c, nil
}

func (dc *driverConnector) Driver() driver.Driver {
	return dc.driver
}

// RegisterConnConfig registers a ConnConfig and returns the connection string to use with Open.
func RegisterConnConfig(c *pgx.ConnConfig) string {
	return pgxDriver.registerConnConfig(c)
}

// UnregisterConnConfig removes the ConnConfig registration for connStr.
func UnregisterConnConfig(connStr string) {
	pgxDriver.unregisterConnConfig(connStr)
}

type Conn struct {
	conn       *pgx.Conn
	psCount    int64 // Counter used for creating unique prepared statement names
	driver     *Driver
	connConfig pgx.ConnConfig
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.conn.IsClosed() {
		return nil, driver.ErrBadConn
	}

	name := fmt.Sprintf("pgx_%d", c.psCount)
	c.psCount++

	sd, err := c.conn.Prepare(ctx, name, query)
	if err != nil {
		return nil, err
	}

	return &Stmt{sd: sd, conn: c}, nil
}

func (c *Conn) Close() error {
	return c.conn.Close(context.Background())
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.conn.IsClosed() {
		return nil, driver.ErrBadConn
	}

	if pconn, ok := ctx.Value(ctxKeyFakeTx).(**pgx.Conn); ok {
		*pconn = c.conn
		return fakeTx{}, nil
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
		return nil, errors.Errorf("unsupported isolation: %v", opts.Isolation)
	}

	if opts.ReadOnly {
		pgxOpts.AccessMode = pgx.ReadOnly
	}

	tx, err := c.conn.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	return wrapTx{tx: tx}, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	if c.conn.IsClosed() {
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

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if c.conn.IsClosed() {
		return nil, driver.ErrBadConn
	}

	args := []interface{}{databaseSQLResultFormats}
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
	return &Rows{conn: c, rows: rows, skipNext: true, skipNextMore: more}, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	if c.conn.IsClosed() {
		return driver.ErrBadConn
	}

	return c.conn.Ping(ctx)
}

type Stmt struct {
	sd   *pgconn.StatementDescription
	conn *Conn
}

func (s *Stmt) Close() error {
	return s.conn.conn.Deallocate(context.Background(), s.sd.Name)
}

func (s *Stmt) NumInput() int {
	return len(s.sd.ParamOIDs)
}

func (s *Stmt) Exec(argsV []driver.Value) (driver.Result, error) {
	return nil, errors.New("Stmt.Exec deprecated and not implemented")
}

func (s *Stmt) ExecContext(ctx context.Context, argsV []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.sd.Name, argsV)
}

func (s *Stmt) Query(argsV []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Stmt.Query deprecated and not implemented")
}

func (s *Stmt) QueryContext(ctx context.Context, argsV []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.sd.Name, argsV)
}

type Rows struct {
	conn         *Conn
	rows         pgx.Rows
	values       []interface{}
	skipNext     bool
	skipNextMore bool
}

func (r *Rows) Columns() []string {
	fieldDescriptions := r.rows.FieldDescriptions()
	names := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		names = append(names, string(fd.Name))
	}
	return names
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if dt, ok := r.conn.conn.ConnInfo().DataTypeForOID(r.rows.FieldDescriptions()[index].DataTypeOID); ok {
		return strings.ToUpper(dt.Name)
	}

	return ""
}

const varHeaderSize = 4

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
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
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
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
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
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
	case pgtype.VarcharOID, pgtype.BPCharArrayOID, pgtype.TextOID:
		return reflect.TypeOf("")
	case pgtype.BoolOID:
		return reflect.TypeOf(false)
	case pgtype.NumericOID:
		return reflect.TypeOf(float64(0))
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return reflect.TypeOf(time.Time{})
	case pgtype.ByteaOID:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}

func (r *Rows) Close() error {
	r.rows.Close()
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.values == nil {
		r.values = make([]interface{}, len(r.rows.FieldDescriptions()))
		for i, fd := range r.rows.FieldDescriptions() {
			switch fd.DataTypeOID {
			case pgtype.BoolOID:
				r.values[i] = &pgtype.Bool{}
			case pgtype.ByteaOID:
				r.values[i] = &pgtype.Bytea{}
			case pgtype.CIDOID:
				r.values[i] = &pgtype.CID{}
			case pgtype.DateOID:
				r.values[i] = &pgtype.Date{}
			case pgtype.Float4OID:
				r.values[i] = &pgtype.Float4{}
			case pgtype.Float8OID:
				r.values[i] = &pgtype.Float8{}
			case pgtype.Int2OID:
				r.values[i] = &pgtype.Int2{}
			case pgtype.Int4OID:
				r.values[i] = &pgtype.Int4{}
			case pgtype.Int8OID:
				r.values[i] = &pgtype.Int8{}
			case pgtype.JSONOID:
				r.values[i] = &pgtype.JSON{}
			case pgtype.JSONBOID:
				r.values[i] = &pgtype.JSONB{}
			case pgtype.OIDOID:
				r.values[i] = &pgtype.OIDValue{}
			case pgtype.TimestampOID:
				r.values[i] = &pgtype.Timestamp{}
			case pgtype.TimestamptzOID:
				r.values[i] = &pgtype.Timestamptz{}
			case pgtype.XIDOID:
				r.values[i] = &pgtype.XID{}
			default:
				r.values[i] = &pgtype.GenericText{}
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

	err := r.rows.Scan(r.values...)
	if err != nil {
		return err
	}

	for i, v := range r.values {
		dest[i], err = v.(driver.Valuer).Value()
		if err != nil {
			return err
		}
	}

	return nil
}

func valueToInterface(argsV []driver.Value) []interface{} {
	args := make([]interface{}, 0, len(argsV))
	for _, v := range argsV {
		if v != nil {
			args = append(args, v.(interface{}))
		} else {
			args = append(args, nil)
		}
	}
	return args
}

func namedValueToInterface(argsV []driver.NamedValue) []interface{} {
	args := make([]interface{}, 0, len(argsV))
	for _, v := range argsV {
		if v.Value != nil {
			args = append(args, v.Value.(interface{}))
		} else {
			args = append(args, nil)
		}
	}
	return args
}

type wrapTx struct{ tx pgx.Tx }

func (wtx wrapTx) Commit() error { return wtx.tx.Commit(context.Background()) }

func (wtx wrapTx) Rollback() error { return wtx.tx.Rollback(context.Background()) }

type fakeTx struct{}

func (fakeTx) Commit() error { return nil }

func (fakeTx) Rollback() error { return nil }

func AcquireConn(db *sql.DB) (*pgx.Conn, error) {
	var conn *pgx.Conn
	ctx := context.WithValue(context.Background(), ctxKeyFakeTx, &conn)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		tx.Rollback()
		return nil, ErrNotPgx
	}

	fakeTxMutex.Lock()
	fakeTxConns[conn] = tx
	fakeTxMutex.Unlock()

	return conn, nil
}

func ReleaseConn(db *sql.DB, conn *pgx.Conn) error {
	var tx *sql.Tx
	var ok bool

	if conn.PgConn().IsBusy() || conn.PgConn().TxStatus() != 'I' {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn.Close(ctx)
	}

	fakeTxMutex.Lock()
	tx, ok = fakeTxConns[conn]
	if ok {
		delete(fakeTxConns, conn)
		fakeTxMutex.Unlock()
	} else {
		fakeTxMutex.Unlock()
		return errors.Errorf("can't release conn that is not acquired")
	}

	return tx.Rollback()
}
