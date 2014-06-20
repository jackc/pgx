package stdlib

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/JackC/pgx"
	"io"
)

func init() {
	d := &Driver{}
	sql.Register("pgx", d)
}

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
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

	return &Stmt{ps: ps, conn: c.conn}, nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	if !c.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	_, err := c.conn.Execute("begin")
	if err != nil {
		return nil, err
	}

	return &Tx{conn: c.conn}, nil
}

type Stmt struct {
	ps   *pgx.PreparedStatement
	conn *pgx.Conn
}

func (s *Stmt) Close() error {
	return s.conn.Deallocate(s.ps.Name)
}

func (s *Stmt) NumInput() int {
	return len(s.ps.ParameterOids)
}

func (s *Stmt) Exec(argsV []driver.Value) (driver.Result, error) {
	if !s.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := valueToInterface(argsV)
	commandTag, err := s.conn.Execute(s.ps.Name, args...)
	return driver.RowsAffected(commandTag.RowsAffected()), err
}

func (s *Stmt) Query(argsV []driver.Value) (driver.Rows, error) {
	if !s.conn.IsAlive() {
		return nil, driver.ErrBadConn
	}

	args := valueToInterface(argsV)

	rowCount := 0
	columnsChan := make(chan []string)
	errChan := make(chan error)
	rowChan := make(chan []driver.Value)

	go func() {
		err := s.conn.SelectFunc(s.ps.Name, func(r *pgx.DataRowReader) error {
			if rowCount == 0 {
				fieldNames := make([]string, len(r.FieldDescriptions))
				for i, fd := range r.FieldDescriptions {
					fieldNames[i] = fd.Name
				}
				columnsChan <- fieldNames
			}
			rowCount++

			values := make([]driver.Value, len(r.FieldDescriptions))
			for i, _ := range r.FieldDescriptions {
				values[i] = r.ReadValue()
			}
			rowChan <- values

			return nil
		}, args...)
		close(rowChan)
		if err != nil {
			errChan <- err
		}
	}()

	rows := Rows{rowChan: rowChan}

	select {
	case rows.columnNames = <-columnsChan:
		return &rows, nil
	case err := <-errChan:
		return nil, err
	}
}

type Rows struct {
	columnNames []string
	rowChan     chan []driver.Value
}

func (r *Rows) Columns() []string {
	return r.columnNames
}

func (r *Rows) Close() error {
	for _ = range r.rowChan {
		// Ensure all rows are read
	}
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	row, ok := <-r.rowChan
	if !ok {
		return io.EOF
	}

	copy(dest, row)
	return nil
}

func valueToInterface(argsV []driver.Value) []interface{} {
	args := make([]interface{}, 0, len(argsV))
	for _, v := range argsV {
		args = append(args, v.(interface{}))
	}
	return args
}

type Tx struct {
	conn *pgx.Conn
}

func (t *Tx) Commit() error {
	_, err := t.conn.Execute("commit")
	return err
}

func (t *Tx) Rollback() error {
	_, err := t.conn.Execute("rollback")
	return err
}
