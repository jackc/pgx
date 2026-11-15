package pgxpool

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
)

// Options supplies pool lifecycle policy separately from connection inputs.
// Nil pointers use defaults; explicit zero lifetime disables expiration. Pool
// sizing is supplied here, not as pool_* connection-string parameters.
type Options struct {
	Connection pgx.ConnectOptions2
	// MaxConns defaults to the greater of 4 or runtime.NumCPU() when zero.
	MaxConns              int32
	MinConns              int32
	MinIdleConns          int32
	MaxConnLifetime       *time.Duration
	MaxConnLifetimeJitter time.Duration
	MaxConnIdleTime       *time.Duration
	// HealthCheckPeriod defaults to one minute when zero.
	HealthCheckPeriod time.Duration
	PingTimeout       time.Duration
	// ConnectTimeout is an outer limit on creation of a physical connection,
	// independent of libpq's per-address connect_timeout. Nil defaults to two
	// minutes; explicit zero disables the outer limit.
	ConnectTimeout *time.Duration
	AfterConnect   func(context.Context, *pgx.Conn) error
	PrepareConn    func(context.Context, *pgx.Conn) (bool, error)
	AfterRelease   func(*pgx.Conn) bool
	BeforeClose    func(*pgx.Conn)
	ShouldPing     func(context.Context, ShouldPingParams) bool
}

// Copy returns independent input values. Callbacks and tracers retain their
// own synchronization obligations when shared across connections.
func (o Options) Copy() Options {
	o.Connection = o.Connection.Copy()
	for _, field := range []**time.Duration{&o.MaxConnLifetime, &o.MaxConnIdleTime, &o.ConnectTimeout} {
		if *field != nil {
			v := **field
			*field = &v
		}
	}
	return o
}

// New2 creates a pool retaining only original connection inputs. Each physical
// creation (including replacement) calls pgx.Connect2 and resolves fresh external
// inputs. Ordinary checkout reuses live sessions. Config returns captured inputs
// and effective pool settings; use its ConnString and New2Options with New2 to
// create another pool. The snapshot cannot be passed to NewWithConfig.
func New2(ctx context.Context, connString string, opts Options) (*Pool, error) {
	opts = opts.Copy()
	connection := opts.Connection.Copy()
	// All three parse-origin flags remain unset: the pool, pgx, and embedded
	// pgconn snapshots are inspection data, not legacy connection configs.
	config := &Config{
		ConnConfig:            &pgx.ConnConfig{Connect2String: connString, Connect2Options: &connection, Tracer: connection.Tracer},
		New2Options:           &opts,
		MaxConns:              max(4, int32(runtime.NumCPU())),
		MinConns:              opts.MinConns,
		MinIdleConns:          opts.MinIdleConns,
		MaxConnLifetime:       time.Hour,
		MaxConnIdleTime:       30 * time.Minute,
		HealthCheckPeriod:     time.Minute,
		MaxConnLifetimeJitter: opts.MaxConnLifetimeJitter,
		PingTimeout:           opts.PingTimeout,
		AfterConnect:          opts.AfterConnect,
		PrepareConn:           opts.PrepareConn,
		AfterRelease:          opts.AfterRelease,
		BeforeClose:           opts.BeforeClose,
		ShouldPing:            opts.ShouldPing,
	}
	if opts.MaxConns != 0 {
		config.MaxConns = opts.MaxConns
	}
	if opts.HealthCheckPeriod != 0 {
		config.HealthCheckPeriod = opts.HealthCheckPeriod
	}

	for _, pair := range []struct{ src, dst *time.Duration }{{opts.MaxConnLifetime, &config.MaxConnLifetime}, {opts.MaxConnIdleTime, &config.MaxConnIdleTime}} {
		if pair.src != nil {
			*pair.dst = *pair.src
		}
	}

	timeout := 2 * time.Minute
	if opts.ConnectTimeout != nil {
		timeout = *opts.ConnectTimeout
	}
	if config.MaxConns <= 0 || config.MinConns < 0 || config.MinIdleConns < 0 || config.MinConns > config.MaxConns || config.MinIdleConns > config.MaxConns {
		return nil, errors.New("invalid pool size")
	}
	if config.MaxConnLifetime < 0 || config.MaxConnIdleTime < 0 || config.HealthCheckPeriod <= 0 || config.MaxConnLifetimeJitter < 0 || config.PingTimeout < 0 || timeout < 0 {
		return nil, errors.New("invalid pool duration")
	}
	factory := func(ctx context.Context) (*pgx.Conn, error) {
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		return pgx.Connect2(ctx, connString, connection)
	}
	return newPool(ctx, config, factory, connection.Tracer)
}
