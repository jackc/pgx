package pool

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/puddle"
)

const defaultMaxConns = 5

type Pool struct {
	p             *puddle.Pool
	beforeAcquire func(*pgx.Conn) bool
	afterRelease  func(*pgx.Conn) bool
}

type Config struct {
	ConnConfig *pgx.ConnConfig

	// BeforeAcquire is called before before a connection is acquired from the pool. It must return true to allow the
	// acquision or false to indicate that the connection should be destroyed and a different connection should be
	// acquired.
	BeforeAcquire func(*pgx.Conn) bool

	// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
	// return the connection to the pool or false to destroy the connection.
	AfterRelease func(*pgx.Conn) bool

	MaxConns int32
}

// Connect creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection.
func Connect(ctx context.Context, connString string) (*Pool, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return ConnectConfig(ctx, config)
}

// ConnectConfig creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection.
func ConnectConfig(ctx context.Context, config *Config) (*Pool, error) {
	p := &Pool{
		beforeAcquire: config.BeforeAcquire,
		afterRelease:  config.AfterRelease,
	}

	p.p = puddle.NewPool(
		func(ctx context.Context) (interface{}, error) { return pgx.ConnectConfig(ctx, config.ConnConfig) },
		func(value interface{}) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			value.(*pgx.Conn).Close(ctx)
			cancel()
		},
		config.MaxConns,
	)

	// Initially establish one connection
	res, err := p.p.Acquire(ctx)
	if err != nil {
		p.p.Close()
		return nil, err
	}
	res.Release()

	return p, nil
}

func ParseConfig(connString string) (*Config, error) {
	connConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config := &Config{ConnConfig: connConfig, MaxConns: defaultMaxConns}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_max_conns"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_max_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_max_conns: %v", err)
		}
		config.MaxConns = int32(n)
	}

	return config, nil
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
func (p *Pool) Close() {
	p.p.Close()
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	for {
		res, err := p.p.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		if p.beforeAcquire == nil || p.beforeAcquire(res.Value().(*pgx.Conn)) {
			return &Conn{res: res, p: p}, nil
		}

		res.Destroy()
	}
}

// AcquireAllIdle atomically acquires all currently idle connections. Its intended use is for health check and
// keep-alive functionality. It does not update pool statistics.
func (p *Pool) AcquireAllIdle() []*Conn {
	resources := p.p.AcquireAllIdle()
	conns := make([]*Conn, 0, len(resources))
	for _, res := range resources {
		if p.beforeAcquire == nil || p.beforeAcquire(res.Value().(*pgx.Conn)) {
			conns = append(conns, &Conn{res: res, p: p})
		} else {
			res.Destroy()
		}
	}

	return conns
}

func (p *Pool) Stat() *Stat {
	return &Stat{s: p.p.Stat()}
}

func (p *Pool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Release()

	return c.Exec(ctx, sql, arguments...)
}

func (p *Pool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errRows{err: err}, err
	}

	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		c.Release()
		return errRows{err: err}, err
	}

	return &poolRows{r: rows, c: c}, nil
}

func (p *Pool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errRow{err: err}
	}

	row := c.QueryRow(ctx, sql, args...)
	return &poolRow{r: row, c: c}
}

func (p *Pool) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	c, err := p.Acquire(ctx)
	if err != nil {
		return errBatchResults{err: err}
	}

	br := c.SendBatch(ctx, b)
	return &poolBatchResults{br: br, c: c}
}

func (p *Pool) Begin(ctx context.Context, txOptions *pgx.TxOptions) (*Tx, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	t, err := c.Begin(ctx, txOptions)
	if err != nil {
		return nil, err
	}

	return &Tx{t: t, c: c}, err
}

func (p *Pool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()

	return c.Conn().CopyFrom(ctx, tableName, columnNames, rowSrc)
}
