package pool

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx"
	"github.com/jackc/puddle"
)

type Pool struct {
	p *puddle.Pool
}

// Connect creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection.
func Connect(ctx context.Context, connString string) (*Pool, error) {
	p := &Pool{}

	maxConnections := 5 // TODO - unhard-code
	p.p = puddle.NewPool(
		func(ctx context.Context) (interface{}, error) { return pgx.Connect(ctx, connString) },
		func(value interface{}) { value.(*pgx.Conn).Close() },
		maxConnections)

	// Initially establish one connection
	res, err := p.p.Acquire(ctx)
	if err != nil {
		p.p.Close()
		return nil, err
	}
	res.Release()

	return p, nil
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
func (p *Pool) Close() {
	p.p.Close()
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	res, err := p.p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return &Conn{res: res}, nil
}

func (p *Pool) Stat() *Stat {
	return &Stat{s: p.p.Stat()}
}

func (p *Pool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return "", err
	}
	defer c.Release()

	return c.Exec(ctx, sql, arguments...)
}

func (p *Pool) Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (*Rows, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return &Rows{err: err}, err
	}

	rows, err := c.Query(ctx, sql, optionsAndArgs...)
	if err == nil {
		rows.c = c
	} else {
		c.Release()
	}

	return rows, err
}

func (p *Pool) QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) *Row {
	c, err := p.Acquire(ctx)
	if err != nil {
		return &Row{err: err}
	}

	row := c.QueryRow(ctx, sql, optionsAndArgs...)
	row.c = c
	return row
}

func (p *Pool) Begin() (*Tx, error) {
	c, err := p.Acquire(context.Background())
	if err != nil {
		return nil, err
	}

	t, err := c.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{t: t, c: c}, err
}
