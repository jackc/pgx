package pool

import (
	"context"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx"
	"github.com/jackc/puddle"
)

// Conn is an acquired *pgx.Conn from a Pool.
type Conn struct {
	res *puddle.Resource
}

// Release returns c to the pool it was acquired from. Once Release has been called, other methods must not be called.
// However, it is safe to call Release multiple times. Subsequent calls after the first will be ignored.
func (c *Conn) Release() {
	if c.res == nil {
		return
	}

	conn := c.Conn()
	res := c.res
	c.res = nil

	go func() {
		if !conn.IsAlive() {
			res.Destroy()
			return
		}

		if conn.TxStatus() != 'I' {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := conn.Exec(ctx, "rollback")
			cancel()
			if err != nil {
				res.Destroy()
				return
			}
		}

		if conn.IsAlive() {
			res.Release()
		} else {
			res.Destroy()
		}
	}()
}

func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn := c.res.Value().(*pgx.Conn)
	return conn.Exec(ctx, sql, arguments...)
}

func (c *Conn) Query(sql string, args ...interface{}) (*Rows, error) {
	r, err := c.res.Value().(*pgx.Conn).Query(sql, args...)
	rows := &Rows{r: r, err: err}
	return rows, err
}

func (c *Conn) QueryEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (*Rows, error) {
	r, err := c.res.Value().(*pgx.Conn).QueryEx(ctx, sql, options, args...)
	rows := &Rows{r: r, err: err}
	return rows, err
}

func (c *Conn) QueryRow(sql string, args ...interface{}) *Row {
	r := c.res.Value().(*pgx.Conn).QueryRow(sql, args...)
	return &Row{r: r}
}

func (c *Conn) QueryRowEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) *Row {
	r := c.res.Value().(*pgx.Conn).QueryRowEx(ctx, sql, options, args...)
	return &Row{r: r}
}

func (c *Conn) Begin() (*pgx.Tx, error) {
	return c.res.Value().(*pgx.Conn).Begin()
}

func (c *Conn) Conn() *pgx.Conn {
	return c.res.Value().(*pgx.Conn)
}
