package pool

import (
	"github.com/jackc/pgx"
)

type Rows struct {
	r   *pgx.Rows
	c   *Conn
	err error
}

func (rows *Rows) Close() {
	rows.r.Close()
	if rows.c != nil {
		rows.c.Release()
		rows.c = nil
	}
}

func (rows *Rows) Err() error {
	if rows.err != nil {
		return rows.err
	}
	return rows.r.Err()
}

func (rows *Rows) FieldDescriptions() []pgx.FieldDescription {
	return rows.r.FieldDescriptions()
}

func (rows *Rows) Next() bool {
	if rows.err != nil {
		return false
	}

	n := rows.r.Next()
	if !n {
		rows.Close()
	}
	return n
}

func (rows *Rows) Scan(dest ...interface{}) error {
	err := rows.r.Scan(dest...)
	if err != nil {
		rows.Close()
	}
	return err
}

func (rows *Rows) Values() ([]interface{}, error) {
	values, err := rows.r.Values()
	if err != nil {
		rows.Close()
	}
	return values, err
}

type Row struct {
	r   *pgx.Row
	c   *Conn
	err error
}

func (row *Row) Scan(dest ...interface{}) error {
	if row.err != nil {
		return row.err
	}

	err := row.r.Scan(dest...)
	if row.c != nil {
		row.c.Release()
	}
	return err
}
