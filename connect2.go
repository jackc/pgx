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
	AfterConnect             func(context.Context, *Conn) error
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
// diagnostics. Config returns captured inputs for reconnecting through Connect2;
// it is an inspection snapshot and cannot be used with legacy constructors.
func Connect2(ctx context.Context, connString string, opts ConnectOptions2) (c *Conn, err error) {
	opts = opts.Copy()
	// Leave both parse-origin flags unset so neither this snapshot nor its
	// embedded pgconn.Config can be used with legacy constructors.
	config := &ConnConfig{Connect2String: connString, Connect2Options: &opts, Tracer: opts.Tracer}
	if t, ok := opts.Tracer.(ConnectTracer); ok {
		ctx = t.TraceConnectStart(ctx, TraceConnectStartData{ConnConfig: config.Copy()})
		defer func() {
			t.TraceConnectEnd(ctx, TraceConnectEndData{Conn: c, Err: err})
		}()
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	for _, n := range []*int{opts.StatementCacheCapacity, opts.DescriptionCacheCapacity} {
		if n != nil && *n < 0 {
			return nil, errors.New("negative cache capacity")
		}
	}
	if opts.DefaultQueryExecMode != nil && (*opts.DefaultQueryExecMode < QueryExecModeCacheStatement || *opts.DefaultQueryExecMode > QueryExecModeSimpleProtocol) {
		return nil, errors.New("invalid query execution mode")
	}
	c = &Conn{
		config:             config,
		typeMap:            pgtype.NewMap(),
		queryTracer:        opts.Tracer,
		preparedStatements: make(map[string]*pgconn.StatementDescription),
		doneChan:           make(chan struct{}),
		closedChan:         make(chan error),
		wbuf:               make([]byte, 0, 1024),
	}
	// Install notification delivery before connecting so setup hooks cannot
	// consume notifications before pgx starts buffering them. Keep the captured
	// input options unchanged, and preserve an explicit application handler.
	physicalOptions := opts.ConnectOptions2
	if physicalOptions.OnNotification == nil {
		physicalOptions.OnNotification = c.bufferNotifications
	}
	low, err := pgconn.Connect2(ctx, connString, physicalOptions)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			low.Close(ctx)
		}
	}()
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
	config.StatementCacheCapacity = statementCapacity
	config.DescriptionCacheCapacity = descriptionCapacity
	config.DefaultQueryExecMode = mode
	c.pgConn = low
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
	if opts.ConnOptions.AfterConnect != nil {
		if err = opts.ConnOptions.AfterConnect(ctx, c); err != nil {
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
