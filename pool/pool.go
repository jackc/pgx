package pool

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/puddle"
	errors "golang.org/x/xerrors"
)

var defaultMinMaxConns = int32(4)
var defaultMaxConnLifetime = time.Hour
var defaultHealthCheckPeriod = time.Minute

type Pool struct {
	p                 *puddle.Pool
	afterConnect      func(context.Context, *pgx.Conn) error
	beforeAcquire     func(*pgx.Conn) bool
	afterRelease      func(*pgx.Conn) bool
	maxConnLifetime   time.Duration
	healthCheckPeriod time.Duration
	closeChan         chan struct{}

	preallocatedConnsMux sync.Mutex
	preallocatedConns    []Conn

	preallocatedPoolRowsMux sync.Mutex
	preallocatedPoolRows    []poolRow
}

// Config is the configuration struct for creating a pool. It is highly recommended to modify a Config returned by
// ParseConfig rather than to construct a Config from scratch.
type Config struct {
	ConnConfig *pgx.ConnConfig

	// AfterConnect is called after a connection is established, but before it is added to the pool.
	AfterConnect func(context.Context, *pgx.Conn) error

	// BeforeAcquire is called before before a connection is acquired from the pool. It must return true to allow the
	// acquision or false to indicate that the connection should be destroyed and a different connection should be
	// acquired.
	BeforeAcquire func(*pgx.Conn) bool

	// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
	// return the connection to the pool or false to destroy the connection.
	AfterRelease func(*pgx.Conn) bool

	// MaxConnLifetime is the duration after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConns is the maximum size of the pool.
	MaxConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration
}

// Connect creates a new Pool and immediately establishes one connection. ctx can be used to cancel this initial
// connection. See ParseConfig for information on connString format.
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
		afterConnect:      config.AfterConnect,
		beforeAcquire:     config.BeforeAcquire,
		afterRelease:      config.AfterRelease,
		maxConnLifetime:   config.MaxConnLifetime,
		healthCheckPeriod: config.HealthCheckPeriod,
		closeChan:         make(chan struct{}),
	}

	p.p = puddle.NewPool(
		func(ctx context.Context) (interface{}, error) {
			conn, err := pgx.ConnectConfig(ctx, config.ConnConfig)
			if err != nil {
				return nil, err
			}

			if p.afterConnect != nil {
				err = p.afterConnect(ctx, conn)
				if err != nil {
					conn.Close(ctx)
					return nil, err
				}
			}

			return conn, nil
		},
		func(value interface{}) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			value.(*pgx.Conn).Close(ctx)
			cancel()
		},
		config.MaxConns,
	)

	go p.backgroundHealthCheck()

	// Initially establish one connection
	res, err := p.p.Acquire(ctx)
	if err != nil {
		p.p.Close()
		return nil, err
	}
	res.Release()

	return p, nil
}

// ParseConfig builds a Config from connString. It parses connString with the same behavior as pgx.ParseConfig with the
// addition of the following variables:
//
// pool_max_conns: integer greater than 0
// pool_max_conn_lifetime: duration string
// pool_health_check_period: duration string
//
// See Config for definitions of these arguments.
//
//   # Example DSN
//   user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
//   # Example URL
//   postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
func ParseConfig(connString string) (*Config, error) {
	connConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config := &Config{ConnConfig: connConfig}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_max_conns"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_max_conns")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Errorf("cannot parse pool_max_conns: %w", err)
		}
		if n < 1 {
			return nil, errors.Errorf("pool_max_conns too small: %d", n)
		}
		config.MaxConns = int32(n)
	} else {
		config.MaxConns = defaultMinMaxConns
		if numCPU := int32(runtime.NumCPU()); numCPU > config.MaxConns {
			config.MaxConns = numCPU
		}
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_max_conn_lifetime"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_max_conn_lifetime")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_max_conn_lifetime: %w", err)
		}
		config.MaxConnLifetime = d
	} else {
		config.MaxConnLifetime = defaultMaxConnLifetime
	}

	if s, ok := config.ConnConfig.Config.RuntimeParams["pool_health_check_period"]; ok {
		delete(connConfig.Config.RuntimeParams, "pool_health_check_period")
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, errors.Errorf("invalid pool_health_check_period: %w", err)
		}
		config.HealthCheckPeriod = d
	} else {
		config.HealthCheckPeriod = defaultHealthCheckPeriod
	}

	return config, nil
}

// Close closes all connections in the pool and rejects future Acquire calls. Blocks until all connections are returned
// to pool and closed.
func (p *Pool) Close() {
	close(p.closeChan)
	p.p.Close()
}

func (p *Pool) backgroundHealthCheck() {
	ticker := time.NewTicker(p.healthCheckPeriod)

	for {
		select {
		case <-p.closeChan:
			ticker.Stop()
			return
		case <-ticker.C:
			p.checkIdleConnsHealth()
		}
	}
}

func (p *Pool) checkIdleConnsHealth() {
	resources := p.p.AcquireAllIdle()

	now := time.Now()
	for _, res := range resources {
		if now.Sub(res.CreationTime()) > p.maxConnLifetime {
			res.Destroy()
		} else {
			res.Release()
		}
	}
}

func (p *Pool) getConn(res *puddle.Resource) *Conn {
	p.preallocatedConnsMux.Lock()

	if len(p.preallocatedConns) == 0 {
		p.preallocatedConns = make([]Conn, 128)
	}

	c := &p.preallocatedConns[len(p.preallocatedConns)-1]
	p.preallocatedConns = p.preallocatedConns[0 : len(p.preallocatedConns)-1]

	p.preallocatedConnsMux.Unlock()

	c.res = res
	c.p = p

	return c
}

func (p *Pool) getPoolRow(c *Conn, r pgx.Row) *poolRow {
	p.preallocatedPoolRowsMux.Lock()

	if len(p.preallocatedPoolRows) == 0 {
		p.preallocatedPoolRows = make([]poolRow, 128)
	}

	pr := &p.preallocatedPoolRows[len(p.preallocatedPoolRows)-1]
	p.preallocatedPoolRows = p.preallocatedPoolRows[0 : len(p.preallocatedPoolRows)-1]

	p.preallocatedPoolRowsMux.Unlock()

	pr.c = c
	pr.r = r

	return pr
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	for {
		res, err := p.p.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		if p.beforeAcquire == nil || p.beforeAcquire(res.Value().(*pgx.Conn)) {
			return p.getConn(res), nil
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
			conns = append(conns, p.getConn(res))
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
	return p.getPoolRow(c, row)
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
