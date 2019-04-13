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
// A DriverConfig can be used to further configure the connection process. This
// allows configuring TLS configuration, setting a custom dialer, logging, and
// setting an AfterConnect hook.
//
//	driverConfig := stdlib.DriverConfig{
// 		ConnConfig: pgx.ConnConfig{
//			Logger:   logger,
//		},
//		AfterConnect: func(c *pgx.Conn) error {
//			// Ensure all connections have this temp table available
//			_, err := c.Exec("create temporary table foo(...)")
//			return err
//		},
//	}
//
//	stdlib.RegisterDriverConfig(&driverConfig)
//
//	db, err := sql.Open("pgx", driverConfig.ConnectionString("postgres://pgx_md5:secret@127.0.0.1:5432/pgx_test"))
//	if err != nil {
//		return err
//	}
//
// pgx uses standard PostgreSQL positional parameters in queries. e.g. $1, $2.
// It does not support named parameters.
//
//	db.QueryRow("select * from users where id=$1", userID)
//
// AcquireConn and ReleaseConn acquire and release a *pgx.Conn from the standard
// database/sql.DB connection pool. This allows operations that must be
// performed on a single connection, but should not be run in a transaction or
// to use pgx specific functionality.
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
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// oids that map to intrinsic database/sql types. These will be allowed to be
// binary, anything else will be forced to text format
var databaseSqlOIDs map[pgtype.OID]bool

var pgxDriver *Driver

type ctxKey int

var ctxKeyFakeTx ctxKey = 0

var ErrNotPgx = errors.New("not pgx *sql.DB")

func init() {
	pgxDriver = &Driver{}
	fakeTxConns = make(map[*pgx.Conn]*sql.Tx)
	sql.Register("pgx", pgxDriver)

	databaseSqlOIDs = make(map[pgtype.OID]bool)
	databaseSqlOIDs[pgtype.BoolOID] = true
	databaseSqlOIDs[pgtype.ByteaOID] = true
	databaseSqlOIDs[pgtype.CIDOID] = true
	databaseSqlOIDs[pgtype.DateOID] = true
	databaseSqlOIDs[pgtype.Float4OID] = true
	databaseSqlOIDs[pgtype.Float8OID] = true
	databaseSqlOIDs[pgtype.Int2OID] = true
	databaseSqlOIDs[pgtype.Int4OID] = true
	databaseSqlOIDs[pgtype.Int8OID] = true
	databaseSqlOIDs[pgtype.OIDOID] = true
	databaseSqlOIDs[pgtype.TimestampOID] = true
	databaseSqlOIDs[pgtype.TimestamptzOID] = true
	databaseSqlOIDs[pgtype.XIDOID] = true
}

var (
	fakeTxMutex sync.Mutex
	fakeTxConns map[*pgx.Conn]*sql.Tx
)

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connConfig, err := pgx.ParseConfig(name)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Ensure eventual timeout
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	c := &Conn{conn: conn, driver: d, connConfig: *connConfig}
	return c, nil
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
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	name := fmt.Sprintf("pgx_%d", c.psCount)
	c.psCount++

	ps, err := c.conn.PrepareEx(ctx, name, query, nil)
	if err != nil {
		return nil, err
	}

	restrictBinaryToDatabaseSqlTypes(ps)

	return &Stmt{ps: ps, conn: c}, nil
}

func (c *Conn) Close() error {
	return c.conn.Close(context.Background())
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if !c.conn.IsAlive() {
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
	case sql.LevelSnapshot:
		pgxOpts.IsoLevel = pgx.RepeatableRead
	case sql.LevelSerializable:
		pgxOpts.IsoLevel = pgx.Serializable
	default:
		return nil, errors.Errorf("unsupported isolation: %v", opts.Isolation)
	}

	if opts.ReadOnly {
		pgxOpts.AccessMode = pgx.ReadOnly
	}

	tx, err := c.conn.BeginEx(ctx, &pgxOpts)
	if err != nil {
		return nil, err
	}

	return wrapTx{tx: tx}, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := namedValueToInterface(argsV)

	commandTag, err := c.conn.Exec(ctx, query, args...)
	// if we got a network error before we had a chance to send the query, retry
	if err != nil && !c.conn.LastStmtSent() {
		if _, is := err.(net.Error); is {
			return nil, driver.ErrBadConn
		}
	}
	return driver.RowsAffected(commandTag.RowsAffected()), err
}

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	var rows pgx.Rows

	if !c.connConfig.PreferSimpleProtocol {
		// TODO - remove hack that creates a new prepared statement for every query -- put in place because of problem preparing empty statement name
		psname := fmt.Sprintf("stdlibpx%v", &argsV)

		ps, err := c.conn.PrepareEx(ctx, psname, query, nil)
		if err != nil {
			// since PrepareEx failed, we didn't actually get to send the values, so
			// we can safely retry
			if _, is := err.(net.Error); is {
				return nil, driver.ErrBadConn
			}
			return nil, err
		}

		restrictBinaryToDatabaseSqlTypes(ps)
		return c.queryPreparedContext(ctx, psname, argsV)
	}

	rows, err := c.conn.Query(ctx, query, namedValueToInterface(argsV)...)
	if err != nil {
		// if we got a network error before we had a chance to send the query, retry
		if !c.conn.LastStmtSent() {
			if _, is := err.(net.Error); is {
				return nil, driver.ErrBadConn
			}
		}
		return nil, err
	}

	// Preload first row because otherwise we won't know what columns are available when database/sql asks.
	more := rows.Next()
	return &Rows{rows: rows, skipNext: true, skipNextMore: more}, nil
}

func (c *Conn) queryPrepared(name string, argsV []driver.Value) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := valueToInterface(argsV)

	rows, err := c.conn.Query(context.Background(), name, args...)
	if err != nil {
		return nil, err
	}

	return &Rows{rows: rows}, nil
}

func (c *Conn) queryPreparedContext(ctx context.Context, name string, argsV []driver.NamedValue) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := namedValueToInterface(argsV)

	rows, err := c.conn.Query(ctx, name, args...)
	if err != nil {
		return nil, err
	}

	// Preload first row because otherwise we won't know what columns are available when database/sql asks.
	more := rows.Next()
	return &Rows{rows: rows, skipNext: true, skipNextMore: more}, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	if !c.conn.IsAlive() {
		return driver.ErrBadConn
	}

	return c.conn.Ping(ctx)
}

// Anything that isn't a database/sql compatible type needs to be forced to
// text format so that pgx.Rows.Values doesn't decode it into a native type
// (e.g. []int32)
func restrictBinaryToDatabaseSqlTypes(ps *pgx.PreparedStatement) {
	for i := range ps.FieldDescriptions {
		intrinsic, _ := databaseSqlOIDs[ps.FieldDescriptions[i].DataType]
		if !intrinsic {
			ps.FieldDescriptions[i].FormatCode = pgx.TextFormatCode
		}
	}
}

type Stmt struct {
	ps   *pgx.PreparedStatement
	conn *Conn
}

func (s *Stmt) Close() error {
	return s.conn.conn.Deallocate(s.ps.Name)
}

func (s *Stmt) NumInput() int {
	return len(s.ps.ParameterOIDs)
}

func (s *Stmt) Exec(argsV []driver.Value) (driver.Result, error) {
	return nil, errors.New("Stmt.Exec deprecated and not implemented")
}

func (s *Stmt) ExecContext(ctx context.Context, argsV []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.ps.Name, argsV)
}

func (s *Stmt) Query(argsV []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Stmt.Query deprecated and not implemented")
}

func (s *Stmt) QueryContext(ctx context.Context, argsV []driver.NamedValue) (driver.Rows, error) {
	return s.conn.queryPreparedContext(ctx, s.ps.Name, argsV)
}

type Rows struct {
	rows         pgx.Rows
	values       []interface{}
	skipNext     bool
	skipNextMore bool
}

func (r *Rows) Columns() []string {
	fieldDescriptions := r.rows.FieldDescriptions()
	names := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		names = append(names, fd.Name)
	}
	return names
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper(r.rows.FieldDescriptions()[index].DataTypeName)
}

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
	return r.rows.FieldDescriptions()[index].Length()
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, ok should be false.
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return r.rows.FieldDescriptions()[index].PrecisionScale()
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return r.rows.FieldDescriptions()[index].Type()
}

func (r *Rows) Close() error {
	r.rows.Close()
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.values == nil {
		r.values = make([]interface{}, len(r.rows.FieldDescriptions()))
		for i, fd := range r.rows.FieldDescriptions() {
			switch fd.DataType {
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

type wrapTx struct{ tx *pgx.Tx }

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
