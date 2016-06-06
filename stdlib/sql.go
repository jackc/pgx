// Package stdlib is the compatibility layer from pgx to database/sql.
//
// A database/sql connection can be established through sql.Open.
//
//	db, err := sql.Open("pgx", "postgres://pgx_md5:secret@localhost:5432/pgx_test?sslmode=disable")
//	if err != nil {
//		return err
//	}
//
// Or a normal pgx connection pool can be established and the database/sql
// connection can be created through stdlib.OpenFromConnPool(). This allows
// more control over the connection process (such as TLS), more control
// over the connection pool, setting an AfterConnect hook, and using both
// database/sql and pgx interfaces as needed.
//
//	connConfig := pgx.ConnConfig{
//		Host:     "localhost",
//		User:     "pgx_md5",
// 		Password: "secret",
// 		Database: "pgx_test",
// 	}
//
//	config := pgx.ConnPoolConfig{ConnConfig: connConfig}
//	pool, err := pgx.NewConnPool(config)
// 	if err != nil {
// 		return err
// 	}
//
//	db, err := stdlib.OpenFromConnPool(pool)
//	if err != nil {
//		t.Fatalf("Unable to create connection pool: %v", err)
//	}
//
// If the database/sql connection is established through
// stdlib.OpenFromConnPool then access to a pgx *ConnPool can be regained
// through db.Driver(). This allows writing a fast path for pgx while
// preserving compatibility with other drivers and database
//
//	if driver, ok := db.Driver().(*stdlib.Driver); ok && driver.Pool != nil {
//		// fast path with pgx
//	} else {
//		// normal path for other drivers and databases
//	}
package stdlib

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx"
)

var openFromConnPoolCount int

// oids that map to intrinsic database/sql types. These will be allowed to be
// binary, anything else will be forced to text format
var databaseSqlOids map[pgx.Oid]bool

func init() {
	d := &Driver{}
	sql.Register("pgx", d)

	databaseSqlOids = make(map[pgx.Oid]bool)
	databaseSqlOids[pgx.BoolOid] = true
	databaseSqlOids[pgx.ByteaOid] = true
	databaseSqlOids[pgx.Int2Oid] = true
	databaseSqlOids[pgx.Int4Oid] = true
	databaseSqlOids[pgx.Int8Oid] = true
	databaseSqlOids[pgx.Float4Oid] = true
	databaseSqlOids[pgx.Float8Oid] = true
	databaseSqlOids[pgx.DateOid] = true
	databaseSqlOids[pgx.TimestampTzOid] = true
	databaseSqlOids[pgx.TimestampOid] = true
}

type Driver struct {
	Pool *pgx.ConnPool
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	if d.Pool != nil {
		conn, err := d.Pool.Acquire()
		if err != nil {
			return nil, err
		}

		return &Conn{conn: conn, pool: d.Pool}, nil
	}

	connConfig, err := pgx.ParseURI(name)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(connConfig)
	if err != nil {
		return nil, err
	}

	c := &Conn{conn: conn}
	return c, nil
}

// OpenFromConnPool takes the existing *pgx.ConnPool pool and returns a *sql.DB
// with pool as the backend. This enables full control over the connection
// process and configuration while maintaining compatibility with the
// database/sql interface. In addition, by calling Driver() on the returned
// *sql.DB and typecasting to *stdlib.Driver a reference to the pgx.ConnPool can
// be reaquired later. This allows fast paths targeting pgx to be used while
// still maintaining compatibility with other databases and drivers.
//
// pool connection size must be at least 2.
func OpenFromConnPool(pool *pgx.ConnPool) (*sql.DB, error) {
	d := &Driver{Pool: pool}
	name := fmt.Sprintf("pgx-%d", openFromConnPoolCount)
	openFromConnPoolCount++
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		return nil, err
	}

	// Presumably OpenFromConnPool is being used because the user wants to use
	// database/sql most of the time, but fast path with pgx some of the time.
	// Allow database/sql to use all the connections, but release 2 idle ones.
	// Don't have database/sql immediately release all idle connections because
	// that would mean that prepared statements would be lost (which kills
	// performance if the prepared statements constantly have to be reprepared)
	stat := pool.Stat()

	if stat.MaxConnections <= 2 {
		return nil, errors.New("pool connection size must be at least 3")
	}
	db.SetMaxIdleConns(stat.MaxConnections - 2)
	db.SetMaxOpenConns(stat.MaxConnections)

	return db, nil
}

type Conn struct {
	conn    *pgx.Conn
	pool    *pgx.ConnPool
	psCount int64 // Counter used for creating unique prepared statement names
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	name := fmt.Sprintf("pgx_%d", c.psCount)
	c.psCount++

	ps, err := c.conn.Prepare(name, query)
	if err != nil {
		return nil, err
	}

	restrictBinaryToDatabaseSqlTypes(ps)

	return &Stmt{ps: ps, conn: c}, nil
}

func (c *Conn) Close() error {
	if c.pool != nil {
		c.pool.Release(c.conn)
		return nil
	}

	return c.conn.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	_, err := c.conn.Exec("begin")
	if err != nil {
		return nil, err
	}

	return &Tx{conn: c.conn}, nil
}

func (c *Conn) Exec(query string, argsV []driver.Value) (driver.Result, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := valueToInterface(argsV)
	commandTag, err := c.conn.Exec(query, args...)
	return driver.RowsAffected(commandTag.RowsAffected()), err
}

func (c *Conn) Query(query string, argsV []driver.Value) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	ps, err := c.conn.Prepare("", query)
	if err != nil {
		return nil, err
	}

	restrictBinaryToDatabaseSqlTypes(ps)

	return c.queryPrepared("", argsV)
}

func (c *Conn) queryPrepared(name string, argsV []driver.Value) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := valueToInterface(argsV)

	rows, err := c.conn.Query(name, args...)
	if err != nil {
		return nil, err
	}

	return &Rows{rows: rows}, nil
}

// Anything that isn't a database/sql compatible type needs to be forced to
// text format so that pgx.Rows.Values doesn't decode it into a native type
// (e.g. []int32)
func restrictBinaryToDatabaseSqlTypes(ps *pgx.PreparedStatement) {
	for i, _ := range ps.FieldDescriptions {
		intrinsic, _ := databaseSqlOids[ps.FieldDescriptions[i].DataType]
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
	return len(s.ps.ParameterOids)
}

func (s *Stmt) Exec(argsV []driver.Value) (driver.Result, error) {
	return s.conn.Exec(s.ps.Name, argsV)
}

func (s *Stmt) Query(argsV []driver.Value) (driver.Rows, error) {
	return s.conn.queryPrepared(s.ps.Name, argsV)
}

// TODO - rename to avoid alloc
type Rows struct {
	rows *pgx.Rows
}

func (r *Rows) Columns() []string {
	fieldDescriptions := r.rows.FieldDescriptions()
	names := make([]string, 0, len(fieldDescriptions))
	for _, fd := range fieldDescriptions {
		names = append(names, fd.Name)
	}
	return names
}

func (r *Rows) Close() error {
	r.rows.Close()
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	more := r.rows.Next()
	if !more {
		if r.rows.Err() == nil {
			return io.EOF
		} else {
			return r.rows.Err()
		}
	}

	values, err := r.rows.Values()
	if err != nil {
		return err
	}

	if len(dest) < len(values) {
		fmt.Printf("%d: %#v\n", len(dest), dest)
		fmt.Printf("%d: %#v\n", len(values), values)
		return errors.New("expected more values than were received")
	}

	for i, v := range values {
		dest[i] = driver.Value(v)
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

type Tx struct {
	conn *pgx.Conn
}

func (t *Tx) Commit() error {
	_, err := t.conn.Exec("commit")
	return err
}

func (t *Tx) Rollback() error {
	_, err := t.conn.Exec("rollback")
	return err
}
