package pgx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/internal/sanitize"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/stmtcache"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// ConnConfig contains all the options used to establish a connection. It must be created by ParseConfig and
// then it can be modified. A manually initialized ConnConfig will cause ConnectConfig to panic.
type ConnConfig struct {
	pgconn.Config
	Logger   Logger
	LogLevel LogLevel

	// Original connection string that was parsed into config.
	connString string

	// BuildStatementCache creates the stmtcache.Cache implementation for connections created with this config. Set
	// to nil to disable automatic prepared statements.
	BuildStatementCache BuildStatementCacheFunc

	// DefaultQueryExecMode controls the default mode for executing queries. By default pgx uses the extended protocol
	// and automatically prepares and caches prepared statements. However, this may be incompatible with proxies such as
	// PGBouncer. In this case it may be preferrable to use QueryExecModeExec or QueryExecModeSimpleProtocol. The same
	// functionality can be controlled on a per query basis by passing a QueryExecMode as the first query argument.
	DefaultQueryExecMode QueryExecMode

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// Copy returns a deep copy of the config that is safe to use and modify.
// The only exception is the tls.Config:
// according to the tls.Config docs it must not be modified after creation.
func (cc *ConnConfig) Copy() *ConnConfig {
	newConfig := new(ConnConfig)
	*newConfig = *cc
	newConfig.Config = *newConfig.Config.Copy()
	return newConfig
}

// ConnString returns the connection string as parsed by pgx.ParseConfig into pgx.ConnConfig.
func (cc *ConnConfig) ConnString() string { return cc.connString }

// BuildStatementCacheFunc is a function that can be used to create a stmtcache.Cache implementation for connection.
type BuildStatementCacheFunc func(conn *pgconn.PgConn) stmtcache.Cache

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage. Use a connection pool to manage access
// to multiple database connections from multiple goroutines.
type Conn struct {
	pgConn             *pgconn.PgConn
	config             *ConnConfig // config used when establishing this connection
	preparedStatements map[string]*pgconn.StatementDescription
	stmtcache          stmtcache.Cache
	logger             Logger
	logLevel           LogLevel

	notifications []*pgconn.Notification

	doneChan   chan struct{}
	closedChan chan error

	typeMap *pgtype.Map

	wbuf []byte
	eqb  extendedQueryBuilder
}

// Identifier a PostgreSQL identifier or name. Identifiers can be composed of
// multiple parts such as ["schema", "table"] or ["table", "column"].
type Identifier []string

// Sanitize returns a sanitized string safe for SQL interpolation.
func (ident Identifier) Sanitize() string {
	parts := make([]string, len(ident))
	for i := range ident {
		s := strings.ReplaceAll(ident[i], string([]byte{0}), "")
		parts[i] = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// ErrNoRows occurs when rows are expected but none are returned.
var ErrNoRows = errors.New("no rows in result set")

// ErrInvalidLogLevel occurs on attempt to set an invalid log level.
var ErrInvalidLogLevel = errors.New("invalid log level")

// Connect establishes a connection with a PostgreSQL server with a connection string. See
// pgconn.Connect for details.
func Connect(ctx context.Context, connString string) (*Conn, error) {
	connConfig, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return connect(ctx, connConfig)
}

// ConnectConfig establishes a connection with a PostgreSQL server with a configuration struct.
// connConfig must have been created by ParseConfig.
func ConnectConfig(ctx context.Context, connConfig *ConnConfig) (*Conn, error) {
	return connect(ctx, connConfig)
}

// ParseConfig creates a ConnConfig from a connection string. ParseConfig handles all options that pgconn.ParseConfig
// does. In addition, it accepts the following options:
//
// 	statement_cache_capacity
// 		The maximum size of the automatic statement cache. Set to 0 to disable automatic statement caching. Default: 512.
//
// 	statement_cache_mode
// 		Possible values: "prepare" and "describe". "prepare" will create prepared statements on the PostgreSQL server.
// 		"describe" will use the anonymous prepared statement to describe a statement without creating a statement on the
// 		server. "describe" is primarily useful when the environment does not allow prepared statements such as when
// 		running a connection pooler like PgBouncer. Default: "prepare"
//
//	default_query_exec_mode
//		Possible values: "cache_statement", "cache_describe", "describe_exec", "exec", and "simple_protocol". See
//		QueryExecMode constant documentation for the meaning of these values. Default: "cache_statement".
func ParseConfig(connString string) (*ConnConfig, error) {
	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	var buildStatementCache BuildStatementCacheFunc
	statementCacheCapacity := 512
	statementCacheMode := stmtcache.ModePrepare
	if s, ok := config.RuntimeParams["statement_cache_capacity"]; ok {
		delete(config.RuntimeParams, "statement_cache_capacity")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse statement_cache_capacity: %w", err)
		}
		statementCacheCapacity = int(n)
	}

	if s, ok := config.RuntimeParams["statement_cache_mode"]; ok {
		delete(config.RuntimeParams, "statement_cache_mode")
		switch s {
		case "prepare":
			statementCacheMode = stmtcache.ModePrepare
		case "describe":
			statementCacheMode = stmtcache.ModeDescribe
		default:
			return nil, fmt.Errorf("invalid statement_cache_mod: %s", s)
		}
	}

	if statementCacheCapacity > 0 {
		buildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
			return stmtcache.New(conn, statementCacheMode, statementCacheCapacity)
		}
	}

	defaultQueryExecMode := QueryExecModeCacheStatement
	if s, ok := config.RuntimeParams["default_query_exec_mode"]; ok {
		delete(config.RuntimeParams, "default_query_exec_mode")
		switch s {
		case "cache_statement":
			defaultQueryExecMode = QueryExecModeCacheStatement
		case "cache_describe":
			defaultQueryExecMode = QueryExecModeCacheDescribe
		case "describe_exec":
			defaultQueryExecMode = QueryExecModeDescribeExec
		case "exec":
			defaultQueryExecMode = QueryExecModeExec
		case "simple_protocol":
			defaultQueryExecMode = QueryExecModeSimpleProtocol
		default:
			return nil, fmt.Errorf("invalid default_query_exec_mode: %v", err)
		}
	}

	connConfig := &ConnConfig{
		Config:               *config,
		createdByParseConfig: true,
		LogLevel:             LogLevelInfo,
		BuildStatementCache:  buildStatementCache,
		DefaultQueryExecMode: defaultQueryExecMode,
		connString:           connString,
	}

	return connConfig, nil
}

func connect(ctx context.Context, config *ConnConfig) (c *Conn, err error) {
	// Default values are set in ParseConfig. Enforce initial creation by ParseConfig rather than setting defaults from
	// zero values.
	if !config.createdByParseConfig {
		panic("config must be created by ParseConfig")
	}
	originalConfig := config

	// This isn't really a deep copy. But it is enough to avoid the config.Config.OnNotification mutation from affecting
	// other connections with the same config. See https://github.com/jackc/pgx/issues/618.
	{
		configCopy := *config
		config = &configCopy
	}

	c = &Conn{
		config:   originalConfig,
		typeMap:  pgtype.NewMap(),
		logLevel: config.LogLevel,
		logger:   config.Logger,
	}

	// Only install pgx notification system if no other callback handler is present.
	if config.Config.OnNotification == nil {
		config.Config.OnNotification = c.bufferNotifications
	} else {
		if c.shouldLog(LogLevelDebug) {
			c.log(ctx, LogLevelDebug, "pgx notification handler disabled by application supplied OnNotification", map[string]interface{}{"host": config.Config.Host})
		}
	}

	if c.shouldLog(LogLevelInfo) {
		c.log(ctx, LogLevelInfo, "Dialing PostgreSQL server", map[string]interface{}{"host": config.Config.Host})
	}
	c.pgConn, err = pgconn.ConnectConfig(ctx, &config.Config)
	if err != nil {
		if c.shouldLog(LogLevelError) {
			c.log(ctx, LogLevelError, "connect failed", map[string]interface{}{"err": err})
		}
		return nil, err
	}

	c.preparedStatements = make(map[string]*pgconn.StatementDescription)
	c.doneChan = make(chan struct{})
	c.closedChan = make(chan error)
	c.wbuf = make([]byte, 0, 1024)

	if c.config.BuildStatementCache != nil {
		c.stmtcache = c.config.BuildStatementCache(c.pgConn)
	}

	// Replication connections can't execute the queries to
	// populate the c.PgTypes and c.pgsqlAfInet
	if _, ok := config.Config.RuntimeParams["replication"]; ok {
		return c, nil
	}

	return c, nil
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close(ctx context.Context) error {
	if c.IsClosed() {
		return nil
	}

	err := c.pgConn.Close(ctx)
	if c.shouldLog(LogLevelInfo) {
		c.log(ctx, LogLevelInfo, "closed connection", nil)
	}
	return err
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
//
// Prepare is idempotent; i.e. it is safe to call Prepare multiple times with the same
// name and sql arguments. This allows a code path to Prepare and Query/Exec without
// concern for if the statement has already been prepared.
func (c *Conn) Prepare(ctx context.Context, name, sql string) (sd *pgconn.StatementDescription, err error) {
	if name != "" {
		var ok bool
		if sd, ok = c.preparedStatements[name]; ok && sd.SQL == sql {
			return sd, nil
		}
	}

	if c.shouldLog(LogLevelError) {
		defer func() {
			if err != nil {
				c.log(ctx, LogLevelError, "Prepare failed", map[string]interface{}{"err": err, "name": name, "sql": sql})
			}
		}()
	}

	sd, err = c.pgConn.Prepare(ctx, name, sql, nil)
	if err != nil {
		return nil, err
	}

	if name != "" {
		c.preparedStatements[name] = sd
	}

	return sd, nil
}

// Deallocate released a prepared statement
func (c *Conn) Deallocate(ctx context.Context, name string) error {
	delete(c.preparedStatements, name)
	_, err := c.pgConn.Exec(ctx, "deallocate "+quoteIdentifier(name)).ReadAll()
	return err
}

func (c *Conn) bufferNotifications(_ *pgconn.PgConn, n *pgconn.Notification) {
	c.notifications = append(c.notifications, n)
}

// WaitForNotification waits for a PostgreSQL notification. It wraps the underlying pgconn notification system in a
// slightly more convenient form.
func (c *Conn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	var n *pgconn.Notification

	// Return already received notification immediately
	if len(c.notifications) > 0 {
		n = c.notifications[0]
		c.notifications = c.notifications[1:]
		return n, nil
	}

	err := c.pgConn.WaitForNotification(ctx)
	if len(c.notifications) > 0 {
		n = c.notifications[0]
		c.notifications = c.notifications[1:]
	}
	return n, err
}

// IsClosed reports if the connection has been closed.
func (c *Conn) IsClosed() bool {
	return c.pgConn.IsClosed()
}

func (c *Conn) die(err error) {
	if c.IsClosed() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // force immediate hard cancel
	c.pgConn.Close(ctx)
}

func (c *Conn) shouldLog(lvl LogLevel) bool {
	return c.logger != nil && c.logLevel >= lvl
}

func (c *Conn) log(ctx context.Context, lvl LogLevel, msg string, data map[string]interface{}) {
	if data == nil {
		data = map[string]interface{}{}
	}
	if c.pgConn != nil && c.pgConn.PID() != 0 {
		data["pid"] = c.pgConn.PID()
	}

	c.logger.Log(ctx, lvl, msg, data)
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Ping executes an empty sql statement against the *Conn
// If the sql returns without error, the database Ping is considered successful, otherwise, the error is returned.
func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.Exec(ctx, ";")
	return err
}

// PgConn returns the underlying *pgconn.PgConn. This is an escape hatch method that allows lower level access to the
// PostgreSQL connection than pgx exposes.
//
// It is strongly recommended that the connection be idle (no in-progress queries) before the underlying *pgconn.PgConn
// is used and the connection must be returned to the same state before any *pgx.Conn methods are again used.
func (c *Conn) PgConn() *pgconn.PgConn { return c.pgConn }

// StatementCache returns the statement cache used for this connection.
func (c *Conn) StatementCache() stmtcache.Cache { return c.stmtcache }

// TypeMap returns the connection info used for this connection.
func (c *Conn) TypeMap() *pgtype.Map { return c.typeMap }

// Config returns a copy of config that was used to establish this connection.
func (c *Conn) Config() *ConnConfig { return c.config.Copy() }

// Exec executes sql. sql can be either a prepared statement name or an SQL string. arguments should be referenced
// positionally from the sql string as $1, $2, etc.
func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	startTime := time.Now()

	commandTag, err := c.exec(ctx, sql, arguments...)
	if err != nil {
		if c.shouldLog(LogLevelError) {
			c.log(ctx, LogLevelError, "Exec", map[string]interface{}{"sql": sql, "args": logQueryArgs(arguments), "err": err})
		}
		return commandTag, err
	}

	if c.shouldLog(LogLevelInfo) {
		endTime := time.Now()
		c.log(ctx, LogLevelInfo, "Exec", map[string]interface{}{"sql": sql, "args": logQueryArgs(arguments), "time": endTime.Sub(startTime), "commandTag": commandTag})
	}

	return commandTag, err
}

func (c *Conn) exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	simpleProtocol := c.config.DefaultQueryExecMode == QueryExecModeSimpleProtocol

optionLoop:
	for len(arguments) > 0 {
		switch arg := arguments[0].(type) {
		case QueryExecMode:
			simpleProtocol = arg == QueryExecModeSimpleProtocol
			arguments = arguments[1:]
		default:
			break optionLoop
		}
	}

	if sd, ok := c.preparedStatements[sql]; ok {
		return c.execPrepared(ctx, sd, arguments)
	}

	if simpleProtocol {
		return c.execSimpleProtocol(ctx, sql, arguments)
	}

	if len(arguments) == 0 {
		return c.execSimpleProtocol(ctx, sql, arguments)
	}

	if c.stmtcache != nil {
		sd, err := c.stmtcache.Get(ctx, sql)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		if c.stmtcache.Mode() == stmtcache.ModeDescribe {
			return c.execParams(ctx, sd, arguments)
		}
		return c.execPrepared(ctx, sd, arguments)
	}

	sd, err := c.Prepare(ctx, "", sql)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return c.execPrepared(ctx, sd, arguments)
}

func (c *Conn) execSimpleProtocol(ctx context.Context, sql string, arguments []interface{}) (commandTag pgconn.CommandTag, err error) {
	if len(arguments) > 0 {
		sql, err = c.sanitizeForSimpleQuery(sql, arguments...)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}

	mrr := c.pgConn.Exec(ctx, sql)
	for mrr.NextResult() {
		commandTag, err = mrr.ResultReader().Close()
	}
	err = mrr.Close()
	return commandTag, err
}

func (c *Conn) execParamsAndPreparedPrefix(sd *pgconn.StatementDescription, arguments []interface{}) error {
	if len(sd.ParamOIDs) != len(arguments) {
		return fmt.Errorf("expected %d arguments, got %d", len(sd.ParamOIDs), len(arguments))
	}

	c.eqb.Reset()

	args, err := convertDriverValuers(arguments)
	if err != nil {
		return err
	}

	for i := range args {
		err = c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], args[i])
		if err != nil {
			return err
		}
	}

	for i := range sd.Fields {
		c.eqb.AppendResultFormat(c.TypeMap().FormatCodeForOID(sd.Fields[i].DataTypeOID))
	}

	return nil
}

func (c *Conn) execParams(ctx context.Context, sd *pgconn.StatementDescription, arguments []interface{}) (pgconn.CommandTag, error) {
	err := c.execParamsAndPreparedPrefix(sd, arguments)
	if err != nil {
		return pgconn.CommandTag{}, err
	}

	result := c.pgConn.ExecParams(ctx, sd.SQL, c.eqb.paramValues, sd.ParamOIDs, c.eqb.paramFormats, c.eqb.resultFormats).Read()
	c.eqb.Reset() // Allow c.eqb internal memory to be GC'ed as soon as possible.
	return result.CommandTag, result.Err
}

func (c *Conn) execPrepared(ctx context.Context, sd *pgconn.StatementDescription, arguments []interface{}) (pgconn.CommandTag, error) {
	err := c.execParamsAndPreparedPrefix(sd, arguments)
	if err != nil {
		return pgconn.CommandTag{}, err
	}

	result := c.pgConn.ExecPrepared(ctx, sd.Name, c.eqb.paramValues, c.eqb.paramFormats, c.eqb.resultFormats).Read()
	c.eqb.Reset() // Allow c.eqb internal memory to be GC'ed as soon as possible.
	return result.CommandTag, result.Err
}

func (c *Conn) getRows(ctx context.Context, sql string, args []interface{}) *connRows {
	r := &connRows{}

	r.ctx = ctx
	r.logger = c
	r.typeMap = c.typeMap
	r.startTime = time.Now()
	r.sql = sql
	r.args = args
	r.conn = c

	return r
}

type QueryExecMode int32

const (
	_ QueryExecMode = iota

	// Automatically prepare and cache statements. This uses the extended protocol. Queries are executed in a single
	// round trip after the statement is cached. This is the default.
	QueryExecModeCacheStatement

	// Cache statement descriptions (i.e. argument and result types) and assume they do not change. This uses the
	// extended protocol. Queries are executed in a single round trip after the description is cached. If the database
	// schema is modified or the search_path is changed this may result in undetected result decoding errors.
	QueryExecModeCacheDescribe

	// Get the statement description on every execution. This uses the extended protocol. Queries require two round trips
	// to execute. It does not use prepared statements (allowing usage with most connection poolers) and is safe even
	// when the the database schema is modified concurrently.
	QueryExecModeDescribeExec

	// Assume the PostgreSQL query parameter types based on the Go type of the arguments. This uses the extended
	// protocol. Queries are executed in a single round trip. Type mappings can be registered with
	// pgtype.Map.RegisterDefaultPgType. Queries will be rejected that have arguments that are unregistered or ambigious.
	// e.g. A map[string]string may have the PostgreSQL type json or hstore. Modes that know the PostgreSQL type can use
	// a map[string]string directly as an argument. This mode cannot.
	QueryExecModeExec

	// Use the simple protocol. Assume the PostgreSQL query parameter types based on the Go type of the arguments.
	// Queries are executed in a single round trip. Type mappings can be registered with
	// pgtype.Map.RegisterDefaultPgType. Queries will be rejected that have arguments that are unregistered or ambigious.
	// e.g. A map[string]string may have the PostgreSQL type json or hstore. Modes that know the PostgreSQL type can use
	// a map[string]string directly as an argument. This mode cannot.
	QueryExecModeSimpleProtocol
)

// QueryResultFormats controls the result format (text=0, binary=1) of a query by result column position.
type QueryResultFormats []int16

// QueryResultFormatsByOID controls the result format (text=0, binary=1) of a query by the result column OID.
type QueryResultFormatsByOID map[uint32]int16

// Query executes sql with args. It is safe to attempt to read from the returned Rows even if an error is returned. The
// error will be the available in rows.Err() after rows are closed. So it is allowed to ignore the error returned from
// Query and handle it in Rows.
//
// Err() on the returned Rows must be checked after the Rows is closed to determine if the query executed successfully
// as some errors can only be detected by reading the entire response. e.g. A divide by zero error on the last row.
//
// For extra control over how the query is executed, the types QuerySimpleProtocol, QueryResultFormats, and
// QueryResultFormatsByOID may be used as the first args to control exactly how the query is executed. This is rarely
// needed. See the documentation for those types for details.
func (c *Conn) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	var resultFormats QueryResultFormats
	var resultFormatsByOID QueryResultFormatsByOID
	simpleProtocol := c.config.DefaultQueryExecMode == QueryExecModeSimpleProtocol

optionLoop:
	for len(args) > 0 {
		switch arg := args[0].(type) {
		case QueryResultFormats:
			resultFormats = arg
			args = args[1:]
		case QueryResultFormatsByOID:
			resultFormatsByOID = arg
			args = args[1:]
		case QueryExecMode:
			simpleProtocol = arg == QueryExecModeSimpleProtocol
			args = args[1:]
		default:
			break optionLoop
		}
	}

	rows := c.getRows(ctx, sql, args)

	var err error
	sd, ok := c.preparedStatements[sql]

	if simpleProtocol && !ok {
		sql, err = c.sanitizeForSimpleQuery(sql, args...)
		if err != nil {
			rows.fatal(err)
			return rows, err
		}

		mrr := c.pgConn.Exec(ctx, sql)
		if mrr.NextResult() {
			rows.resultReader = mrr.ResultReader()
			rows.multiResultReader = mrr
		} else {
			err = mrr.Close()
			rows.fatal(err)
			return rows, err
		}

		return rows, nil
	}

	c.eqb.Reset()

	if !ok {
		if c.stmtcache != nil {
			sd, err = c.stmtcache.Get(ctx, sql)
			if err != nil {
				rows.fatal(err)
				return rows, rows.err
			}
		} else {
			sd, err = c.pgConn.Prepare(ctx, "", sql, nil)
			if err != nil {
				rows.fatal(err)
				return rows, rows.err
			}
		}
	}
	if len(sd.ParamOIDs) != len(args) {
		rows.fatal(fmt.Errorf("expected %d arguments, got %d", len(sd.ParamOIDs), len(args)))
		return rows, rows.err
	}

	rows.sql = sd.SQL

	args, err = convertDriverValuers(args)
	if err != nil {
		rows.fatal(err)
		return rows, rows.err
	}

	for i := range args {
		err = c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], args[i])
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}
	}

	if resultFormatsByOID != nil {
		resultFormats = make([]int16, len(sd.Fields))
		for i := range resultFormats {
			resultFormats[i] = resultFormatsByOID[uint32(sd.Fields[i].DataTypeOID)]
		}
	}

	if resultFormats == nil {
		for i := range sd.Fields {
			c.eqb.AppendResultFormat(c.TypeMap().FormatCodeForOID(sd.Fields[i].DataTypeOID))
		}

		resultFormats = c.eqb.resultFormats
	}

	if c.stmtcache != nil && c.stmtcache.Mode() == stmtcache.ModeDescribe {
		rows.resultReader = c.pgConn.ExecParams(ctx, sql, c.eqb.paramValues, sd.ParamOIDs, c.eqb.paramFormats, resultFormats)
	} else {
		rows.resultReader = c.pgConn.ExecPrepared(ctx, sd.Name, c.eqb.paramValues, c.eqb.paramFormats, resultFormats)
	}

	c.eqb.Reset() // Allow c.eqb internal memory to be GC'ed as soon as possible.

	return rows, rows.err
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned Row. That Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	rows, _ := c.Query(ctx, sql, args...)
	return (*connRow)(rows.(*connRows))
}

// QueryFuncRow is the argument to the QueryFunc callback function.
//
// QueryFuncRow is an interface instead of a struct to allow tests to mock QueryFunc. However, adding a method to an
// interface is technically a breaking change. Because of this the QueryFuncRow interface is partially excluded from
// semantic version requirements. Methods will not be removed or changed, but new methods may be added.
type QueryFuncRow interface {
	FieldDescriptions() []pgproto3.FieldDescription

	// RawValues returns the unparsed bytes of the row values. The returned [][]byte is only valid during the current
	// function call. However, the underlying byte data is safe to retain a reference to and mutate.
	RawValues() [][]byte
}

// QueryFunc executes sql with args. For each row returned by the query the values will scanned into the elements of
// scans and f will be called. If any row fails to scan or f returns an error the query will be aborted and the error
// will be returned.
func (c *Conn) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(QueryFuncRow) error) (pgconn.CommandTag, error) {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		err = f(rows)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
	}

	if err := rows.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}

	return rows.CommandTag(), nil
}

// SendBatch sends all queued queries to the server at once. All queries are run in an implicit transaction unless
// explicit transaction control statements are executed. The returned BatchResults must be closed before the connection
// is used again.
func (c *Conn) SendBatch(ctx context.Context, b *Batch) BatchResults {
	simpleProtocol := c.config.DefaultQueryExecMode == QueryExecModeSimpleProtocol
	var sb strings.Builder
	if simpleProtocol {
		for i, bi := range b.items {
			if i > 0 {
				sb.WriteByte(';')
			}
			sql, err := c.sanitizeForSimpleQuery(bi.query, bi.arguments...)
			if err != nil {
				return &batchResults{ctx: ctx, conn: c, err: err}
			}
			sb.WriteString(sql)
		}
		mrr := c.pgConn.Exec(ctx, sb.String())
		return &batchResults{
			ctx:  ctx,
			conn: c,
			mrr:  mrr,
			b:    b,
			ix:   0,
		}
	}

	distinctUnpreparedQueries := map[string]struct{}{}

	for _, bi := range b.items {
		if _, ok := c.preparedStatements[bi.query]; ok {
			continue
		}
		distinctUnpreparedQueries[bi.query] = struct{}{}
	}

	var stmtCache stmtcache.Cache
	if len(distinctUnpreparedQueries) > 0 {
		if c.stmtcache != nil && c.stmtcache.Cap() >= len(distinctUnpreparedQueries) {
			stmtCache = c.stmtcache
		} else {
			stmtCache = stmtcache.New(c.pgConn, stmtcache.ModeDescribe, len(distinctUnpreparedQueries))
		}

		for sql, _ := range distinctUnpreparedQueries {
			_, err := stmtCache.Get(ctx, sql)
			if err != nil {
				return &batchResults{ctx: ctx, conn: c, err: err}
			}
		}
	}

	batch := &pgconn.Batch{}

	for _, bi := range b.items {
		c.eqb.Reset()

		sd := c.preparedStatements[bi.query]
		if sd == nil {
			var err error
			sd, err = stmtCache.Get(ctx, bi.query)
			if err != nil {
				return &batchResults{ctx: ctx, conn: c, err: err}
			}
		}

		if len(sd.ParamOIDs) != len(bi.arguments) {
			return &batchResults{ctx: ctx, conn: c, err: fmt.Errorf("mismatched param and argument count")}
		}

		args, err := convertDriverValuers(bi.arguments)
		if err != nil {
			return &batchResults{ctx: ctx, conn: c, err: err}
		}

		for i := range args {
			err = c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], args[i])
			if err != nil {
				return &batchResults{ctx: ctx, conn: c, err: err}
			}
		}

		for i := range sd.Fields {
			c.eqb.AppendResultFormat(c.TypeMap().FormatCodeForOID(sd.Fields[i].DataTypeOID))
		}

		if sd.Name == "" {
			batch.ExecParams(bi.query, c.eqb.paramValues, sd.ParamOIDs, c.eqb.paramFormats, c.eqb.resultFormats)
		} else {
			batch.ExecPrepared(sd.Name, c.eqb.paramValues, c.eqb.paramFormats, c.eqb.resultFormats)
		}
	}

	c.eqb.Reset() // Allow c.eqb internal memory to be GC'ed as soon as possible.

	mrr := c.pgConn.ExecBatch(ctx, batch)

	return &batchResults{
		ctx:  ctx,
		conn: c,
		mrr:  mrr,
		b:    b,
		ix:   0,
	}
}

func (c *Conn) sanitizeForSimpleQuery(sql string, args ...interface{}) (string, error) {
	if c.pgConn.ParameterStatus("standard_conforming_strings") != "on" {
		return "", errors.New("simple protocol queries must be run with standard_conforming_strings=on")
	}

	if c.pgConn.ParameterStatus("client_encoding") != "UTF8" {
		return "", errors.New("simple protocol queries must be run with client_encoding=UTF8")
	}

	var err error
	valueArgs := make([]interface{}, len(args))
	for i, a := range args {
		valueArgs[i], err = convertSimpleArgument(c.typeMap, a)
		if err != nil {
			return "", err
		}
	}

	return sanitize.SanitizeSQL(sql, valueArgs...)
}

// LoadType inspects the database for typeName and produces a pgtype.Type suitable for registration.
func (c *Conn) LoadType(ctx context.Context, typeName string) (*pgtype.Type, error) {
	var oid uint32

	err := c.QueryRow(ctx, "select $1::text::regtype::oid;", typeName).Scan(&oid)
	if err != nil {
		return nil, err
	}

	var typtype string

	err = c.QueryRow(ctx, "select typtype::text from pg_type where oid=$1", oid).Scan(&typtype)
	if err != nil {
		return nil, err
	}

	switch typtype {
	case "b": // array
		elementOID, err := c.getArrayElementOID(ctx, oid)
		if err != nil {
			return nil, err
		}

		dt, ok := c.TypeMap().TypeForOID(elementOID)
		if !ok {
			return nil, errors.New("array element OID not registered")
		}

		return &pgtype.Type{Name: typeName, OID: oid, Codec: &pgtype.ArrayCodec{ElementType: dt}}, nil
	case "c": // composite
		fields, err := c.getCompositeFields(ctx, oid)
		if err != nil {
			return nil, err
		}

		return &pgtype.Type{Name: typeName, OID: oid, Codec: &pgtype.CompositeCodec{Fields: fields}}, nil
	case "e": // enum
		return &pgtype.Type{Name: typeName, OID: oid, Codec: &pgtype.EnumCodec{}}, nil
	default:
		return &pgtype.Type{}, errors.New("unknown typtype")
	}
}

func (c *Conn) getArrayElementOID(ctx context.Context, oid uint32) (uint32, error) {
	var typelem uint32

	err := c.QueryRow(ctx, "select typelem from pg_type where oid=$1", oid).Scan(&typelem)
	if err != nil {
		return 0, err
	}

	return typelem, nil
}

func (c *Conn) getCompositeFields(ctx context.Context, oid uint32) ([]pgtype.CompositeCodecField, error) {
	var typrelid uint32

	err := c.QueryRow(ctx, "select typrelid from pg_type where oid=$1", oid).Scan(&typrelid)
	if err != nil {
		return nil, err
	}

	var fields []pgtype.CompositeCodecField
	var fieldName string
	var fieldOID uint32
	_, err = c.QueryFunc(ctx, `select attname, atttypid
from pg_attribute
where attrelid=$1
order by attnum`,
		[]interface{}{typrelid},
		[]interface{}{&fieldName, &fieldOID},
		func(qfr QueryFuncRow) error {
			dt, ok := c.TypeMap().TypeForOID(fieldOID)
			if !ok {
				return fmt.Errorf("unknown composite type field OID: %v", fieldOID)
			}
			fields = append(fields, pgtype.CompositeCodecField{Name: fieldName, Type: dt})
			return nil
		})
	if err != nil {
		return nil, err
	}

	return fields, nil
}
