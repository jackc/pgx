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
package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// oids that map to intrinsic database/sql types. These will be allowed to be
// binary, anything else will be forced to text format
var databaseSqlOids map[pgtype.Oid]bool

var pgxDriver *Driver

func init() {
	pgxDriver = &Driver{
		configs: make(map[int64]*DriverConfig),
	}
	sql.Register("pgx", pgxDriver)

	databaseSqlOids = make(map[pgtype.Oid]bool)
	databaseSqlOids[pgtype.BoolOid] = true
	databaseSqlOids[pgtype.ByteaOid] = true
	databaseSqlOids[pgtype.CidOid] = true
	databaseSqlOids[pgtype.DateOid] = true
	databaseSqlOids[pgtype.Float4Oid] = true
	databaseSqlOids[pgtype.Float8Oid] = true
	databaseSqlOids[pgtype.Int2Oid] = true
	databaseSqlOids[pgtype.Int4Oid] = true
	databaseSqlOids[pgtype.Int8Oid] = true
	databaseSqlOids[pgtype.OidOid] = true
	databaseSqlOids[pgtype.TimestampOid] = true
	databaseSqlOids[pgtype.TimestamptzOid] = true
	databaseSqlOids[pgtype.XidOid] = true
}

type Driver struct {
	configMutex sync.Mutex
	configCount int64
	configs     map[int64]*DriverConfig
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	var connConfig pgx.ConnConfig
	var afterConnect func(*pgx.Conn) error
	if len(name) >= 9 && name[0] == 0 {
		idBuf := []byte(name)[1:9]
		id := int64(binary.BigEndian.Uint64(idBuf))
		connConfig = d.configs[id].ConnConfig
		afterConnect = d.configs[id].AfterConnect
		name = name[9:]
	}

	parsedConfig, err := pgx.ParseConnectionString(name)
	if err != nil {
		return nil, err
	}
	connConfig = connConfig.Merge(parsedConfig)

	conn, err := pgx.Connect(connConfig)
	if err != nil {
		return nil, err
	}

	if afterConnect != nil {
		err = afterConnect(conn)
		if err != nil {
			return nil, err
		}
	}

	c := &Conn{conn: conn}
	return c, nil
}

type DriverConfig struct {
	pgx.ConnConfig
	AfterConnect func(*pgx.Conn) error // function to call on every new connection
	driver       *Driver
	id           int64
}

// ConnectionString encodes the DriverConfig into the original connection
// string. DriverConfig must be registered before calling ConnectionString.
func (c *DriverConfig) ConnectionString(original string) string {
	if c.driver == nil {
		panic("DriverConfig must be registered before calling ConnectionString")
	}

	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[1:], uint64(c.id))
	buf = append(buf, original...)
	return string(buf)
}

func (d *Driver) registerDriverConfig(c *DriverConfig) {
	d.configMutex.Lock()

	c.driver = d
	c.id = d.configCount
	d.configs[d.configCount] = c
	d.configCount++

	d.configMutex.Unlock()
}

func (d *Driver) unregisterDriverConfig(c *DriverConfig) {
	d.configMutex.Lock()
	delete(d.configs, c.id)
	d.configMutex.Unlock()
}

// RegisterDriverConfig registers a DriverConfig for use with Open.
func RegisterDriverConfig(c *DriverConfig) {
	pgxDriver.registerDriverConfig(c)
}

// UnregisterDriverConfig removes a DriverConfig registration.
func UnregisterDriverConfig(c *DriverConfig) {
	pgxDriver.unregisterDriverConfig(c)
}

type Conn struct {
	conn    *pgx.Conn
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

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	ps, err := c.conn.PrepareExContext(ctx, "", query, nil)
	if err != nil {
		return nil, err
	}

	restrictBinaryToDatabaseSqlTypes(ps)

	return c.queryPreparedContext(ctx, "", argsV)
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

func (c *Conn) queryPreparedContext(ctx context.Context, name string, argsV []driver.NamedValue) (driver.Rows, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := namedValueToInterface(argsV)

	rows, err := c.conn.QueryEx(ctx, name, nil, args...)
	if err != nil {
		fmt.Println(err)
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

type Rows struct {
	rows   *pgx.Rows
	values []interface{}
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
	if r.values == nil {
		r.values = make([]interface{}, len(r.rows.FieldDescriptions()))
		for i, fd := range r.rows.FieldDescriptions() {
			switch fd.DataType {
			case pgtype.BoolOid:
				r.values[i] = &pgtype.Bool{}
			case pgtype.ByteaOid:
				r.values[i] = &pgtype.Bytea{}
			case pgtype.CidOid:
				r.values[i] = &pgtype.Cid{}
			case pgtype.DateOid:
				r.values[i] = &pgtype.Date{}
			case pgtype.Float4Oid:
				r.values[i] = &pgtype.Float4{}
			case pgtype.Float8Oid:
				r.values[i] = &pgtype.Float8{}
			case pgtype.Int2Oid:
				r.values[i] = &pgtype.Int2{}
			case pgtype.Int4Oid:
				r.values[i] = &pgtype.Int4{}
			case pgtype.Int8Oid:
				r.values[i] = &pgtype.Int8{}
			case pgtype.OidOid:
				r.values[i] = &pgtype.OidValue{}
			case pgtype.TimestampOid:
				r.values[i] = &pgtype.Timestamp{}
			case pgtype.TimestamptzOid:
				r.values[i] = &pgtype.Timestamptz{}
			case pgtype.XidOid:
				r.values[i] = &pgtype.Xid{}
			default:
				r.values[i] = &pgtype.GenericText{}
			}
		}
	}

	more := r.rows.Next()
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
