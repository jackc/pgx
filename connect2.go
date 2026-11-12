package pgx

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/internal/stmtcache"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// ConnOptions supplies pgx execution and initialization settings. Nil scalar
// pointers use the string extension or its default; pointers preserve explicit
// zero capacities. Values are captured when a connection or adapter is created.
type ConnOptions struct {
	StatementCacheCapacity   *int
	DescriptionCacheCapacity *int
	DefaultQueryExecMode     *QueryExecMode
	Tracer                   QueryTracer
	AfterConnect             func(context.Context, *Conn2) error
}

// Copy returns independent input values. Tracer and callback implementations
// remain responsible for their own concurrent use.
func (o ConnOptions) Copy() ConnOptions {
	if o.StatementCacheCapacity != nil {
		v := *o.StatementCacheCapacity
		o.StatementCacheCapacity = &v
	}
	if o.DescriptionCacheCapacity != nil {
		v := *o.DescriptionCacheCapacity
		o.DescriptionCacheCapacity = &v
	}
	if o.DefaultQueryExecMode != nil {
		v := *o.DefaultQueryExecMode
		o.DefaultQueryExecMode = &v
	}
	return o
}

// ConnectOptions2 combines the physical connection capabilities and pgx options.
// Go execution options take precedence over pgx connection-string extensions.
type ConnectOptions2 struct {
	pgconn.ConnectOptions2
	ConnOptions
}

func (o ConnectOptions2) Copy() ConnectOptions2 {
	o.ConnectOptions2 = o.ConnectOptions2.Copy()
	o.ConnOptions = o.ConnOptions.Copy()
	return o
}

// Connect2 establishes a connection with fresh input resolution for each physical
// connection. See pgconn.Connect2. Use PgConn().ConnectionInfo() for destination
// diagnostics and retain the original string and options for reconnecting.
func Connect2(ctx context.Context, connString string, opts ConnectOptions2) (*Conn2, error) {
	opts = opts.Copy()
	return ConnectFunc(ctx, func(ctx context.Context) (*pgconn.PgConn, error) {
		return pgconn.Connect2(ctx, connString, opts.ConnectOptions2)
	}, opts.ConnOptions)
}

// ConnectFunc invokes connect exactly once with the tracing-enriched context.
// connect owns retries and must return a fresh, idle connection, transferring
// exclusive ownership to pgx. On error, either side may return a connection;
// pgx closes it. Notifications buffered by pgconn.Connect2 during setup are
// transferred before AfterConnect. An explicit notification handler wins.
//
// A factory using legacy pgconn.ConnectConfig must install its own notification
// handler before connecting if its hooks consume notifications during setup.
// The returned wrapper exposes no legacy Config getter.
func ConnectFunc(ctx context.Context, connect func(context.Context) (*pgconn.PgConn, error), opts ConnOptions) (*Conn2, error) {
	c, err := connectFunc(ctx, connect, opts)
	if err != nil {
		return nil, err
	}
	return c.AsConn2(), nil
}

func connectFunc(ctx context.Context, connect func(context.Context) (*pgconn.PgConn, error), opts ConnOptions) (c *Conn, err error) {
	opts = opts.Copy()
	if t, ok := opts.Tracer.(ConnectTracer2); ok {
		ctx = t.TraceConnectStart2(ctx)
		defer func() {
			var view *Conn2
			if c != nil {
				view = c.AsConn2()
			}
			t.TraceConnectEnd2(ctx, TraceConnectEndData2{Conn: view, Err: err})
		}()
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if connect == nil {
		return nil, errors.New("nil connection factory")
	}
	for _, n := range []*int{opts.StatementCacheCapacity, opts.DescriptionCacheCapacity} {
		if n != nil && *n < 0 {
			return nil, errors.New("negative cache capacity")
		}
	}
	if opts.DefaultQueryExecMode != nil && (*opts.DefaultQueryExecMode < QueryExecModeCacheStatement || *opts.DefaultQueryExecMode > QueryExecModeSimpleProtocol) {
		return nil, errors.New("invalid query execution mode")
	}
	low, err := connect(ctx)
	if err != nil {
		if low != nil {
			low.Close(ctx)
		}
		return nil, err
	}
	if low == nil {
		return nil, errors.New("connection factory returned nil without an error")
	}
	defer func() {
		if err != nil {
			low.Close(ctx)
		}
	}()
	if low.IsClosed() || low.IsBusy() {
		return nil, errors.New("connection factory returned a closed or busy connection")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	statementCapacity, descriptionCapacity := 512, 512
	mode := QueryExecModeCacheStatement
	for k, dst := range map[string]*int{"statement_cache_capacity": &statementCapacity, "description_cache_capacity": &descriptionCapacity} {
		if v, ok := low.ClientOption(k); ok {
			*dst, err = strconv.Atoi(v)
			if err != nil || *dst < 0 {
				return nil, fmt.Errorf("invalid %s", k)
			}
		}
	}
	if v, ok := low.ClientOption("default_query_exec_mode"); ok {
		m, exists := map[string]QueryExecMode{"cache_statement": QueryExecModeCacheStatement, "cache_describe": QueryExecModeCacheDescribe, "describe_exec": QueryExecModeDescribeExec, "exec": QueryExecModeExec, "simple_protocol": QueryExecModeSimpleProtocol}[v]
		if !exists {
			return nil, errors.New("invalid default_query_exec_mode")
		}
		mode = m
	}
	if opts.StatementCacheCapacity != nil {
		statementCapacity = *opts.StatementCacheCapacity
	}
	if opts.DescriptionCacheCapacity != nil {
		descriptionCapacity = *opts.DescriptionCacheCapacity
	}
	if opts.DefaultQueryExecMode != nil {
		mode = *opts.DefaultQueryExecMode
	}
	c = &Conn{
		pgConn:                   low,
		typeMap:                  pgtype.NewMap(),
		queryTracer:              opts.Tracer,
		statementCacheCapacity:   statementCapacity,
		descriptionCacheCapacity: descriptionCapacity,
		defaultQueryExecMode:     mode,
		preparedStatements:       make(map[string]*pgconn.StatementDescription),
		doneChan:                 make(chan struct{}),
		closedChan:               make(chan error),
		wbuf:                     make([]byte, 0, 1024),
	}
	if t, ok := opts.Tracer.(BatchTracer); ok {
		c.batchTracer = t
	}
	if t, ok := opts.Tracer.(CopyFromTracer); ok {
		c.copyFromTracer = t
	}
	if t, ok := opts.Tracer.(PrepareTracer); ok {
		c.prepareTracer = t
	}
	if statementCapacity > 0 {
		c.statementCache = stmtcache.NewLRUCache(statementCapacity)
	}
	if descriptionCapacity > 0 {
		c.descriptionCache = stmtcache.NewLRUCache(descriptionCapacity)
	}
	low.SetNotificationHandler(c.bufferNotifications)
	if opts.AfterConnect != nil {
		if err = opts.AfterConnect(ctx, c.AsConn2()); err != nil {
			return nil, err
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if low.IsClosed() || low.IsBusy() {
		return nil, errors.New("AfterConnect left connection closed or busy")
	}
	return c, nil
}
