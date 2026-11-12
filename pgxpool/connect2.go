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
	Connection            pgx.ConnectOptions2
	MaxConns              *int32
	MinConns              int32
	MinIdleConns          int32
	MaxConnLifetime       *time.Duration
	MaxConnLifetimeJitter time.Duration
	MaxConnIdleTime       *time.Duration
	HealthCheckPeriod     *time.Duration
	PingTimeout           time.Duration
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

// New2 creates a pool retaining only original connection inputs. Each physical
// creation (including replacement) calls pgx.Connect2 and resolves fresh external
// inputs. Ordinary checkout reuses live sessions. Retain the original inputs
// when constructing another pool; Pool2 exposes no parsed Config.
func New2(ctx context.Context, connString string, opts Options) (*Pool2, error) {
	connection := opts.Connection.Copy()
	config := &Config{
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
	if opts.MaxConns != nil {
		config.MaxConns = *opts.MaxConns
	}

	for _, pair := range []struct{ src, dst *time.Duration }{{opts.MaxConnLifetime, &config.MaxConnLifetime}, {opts.MaxConnIdleTime, &config.MaxConnIdleTime}, {opts.HealthCheckPeriod, &config.HealthCheckPeriod}} {
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
		c, err := pgx.Connect2(ctx, connString, connection)
		if err != nil {
			return nil, err
		}
		return c.LegacyConn(), nil
	}
	p, err := newPool(ctx, config, factory, connection.Tracer, false)
	if err != nil {
		return nil, err
	}
	return p.AsPool2(), nil
}
