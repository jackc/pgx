package pgxpool

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Pool2 shares Pool's lifecycle and query implementation without exposing a
// parsed Config. Retain the original inputs to create another pool.
type Pool2 struct {
	core *Pool
}

// AsPool2 returns a view sharing p's lifetime and ownership.
func (p *Pool) AsPool2() *Pool2 { return &Pool2{core: p} }

// LegacyPool is an explicit bridge for APIs requiring *Pool. Config returns nil
// when the pool was created by New2. Both views share ownership and lifetime.
func (p *Pool2) LegacyPool() *Pool { return p.core }

// Conn2 borrows a connection from Pool2 until Release or Hijack.
type Conn2 struct {
	core *Conn
}

func (c *Conn2) Conn() *pgx.Conn2   { return c.core.Conn().AsConn2() }
func (c *Conn2) Hijack() *pgx.Conn2 { return c.core.Hijack().AsConn2() }
func (p *Pool2) Acquire(ctx context.Context) (*Conn2, error) {
	c, err := p.core.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn2{core: c}, nil
}

func (p *Pool2) AcquireFunc(ctx context.Context, f func(*Conn2) error) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()
	return f(c)
}

func (p *Pool2) AcquireAllIdle(ctx context.Context) []*Conn2 {
	old := p.core.AcquireAllIdle(ctx)
	result := make([]*Conn2, len(old))
	for i, c := range old {
		result[i] = &Conn2{core: c}
	}
	return result
}

// Close delegates to [Pool.Close].
func (p *Pool2) Close() { p.core.Close() }

// Reset delegates to [Pool.Reset].
func (p *Pool2) Reset() { p.core.Reset() }

// Stat delegates to [Pool.Stat].
func (p *Pool2) Stat() *Stat { return p.core.Stat() }

// Exec delegates to [Pool.Exec].
func (p *Pool2) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return p.core.Exec(ctx, sql, arguments...)
}

// Query delegates to [Pool.Query].
func (p *Pool2) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.core.Query(ctx, sql, args...)
}

// QueryRow delegates to [Pool.QueryRow].
func (p *Pool2) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.core.QueryRow(ctx, sql, args...)
}

// SendBatch delegates to [Pool.SendBatch].
func (p *Pool2) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.core.SendBatch(ctx, b)
}

// Begin delegates to [Pool.Begin].
func (p *Pool2) Begin(ctx context.Context) (pgx.Tx, error) { return p.core.Begin(ctx) }

// BeginTx delegates to [Pool.BeginTx].
func (p *Pool2) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return p.core.BeginTx(ctx, txOptions)
}

// CopyFrom delegates to [Pool.CopyFrom].
func (p *Pool2) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.core.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Ping delegates to [Pool.Ping].
func (p *Pool2) Ping(ctx context.Context) error { return p.core.Ping(ctx) }

// Release delegates to [Conn.Release].
func (c *Conn2) Release() { c.core.Release() }

// Exec delegates to [Conn.Exec].
func (c *Conn2) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return c.core.Exec(ctx, sql, arguments...)
}

// Query delegates to [Conn.Query].
func (c *Conn2) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.core.Query(ctx, sql, args...)
}

// QueryRow delegates to [Conn.QueryRow].
func (c *Conn2) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.core.QueryRow(ctx, sql, args...)
}

// SendBatch delegates to [Conn.SendBatch].
func (c *Conn2) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return c.core.SendBatch(ctx, b)
}

// CopyFrom delegates to [Conn.CopyFrom].
func (c *Conn2) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return c.core.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Begin delegates to [Conn.Begin].
func (c *Conn2) Begin(ctx context.Context) (pgx.Tx, error) { return c.core.Begin(ctx) }

// BeginTx delegates to [Conn.BeginTx].
func (c *Conn2) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return c.core.BeginTx(ctx, txOptions)
}

// Ping delegates to [Conn.Ping].
func (c *Conn2) Ping(ctx context.Context) error { return c.core.Ping(ctx) }
