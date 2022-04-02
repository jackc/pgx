package pgx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/internal/anynil"
	"github.com/jackc/pgx/v5/internal/sanitize"
	"github.com/jackc/pgx/v5/internal/stmtcache"
	"github.com/jackc/pgx/v5/pgconn"
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

	// StatementCacheCapacity is maximum size of the statement cache used when executing a query with "cache_statement"
	// query exec mode.
	StatementCacheCapacity int

	// DescriptionCacheCapacity is the maximum size of the description cache used when executing a query with
	// "cache_describe" query exec mode.
	DescriptionCacheCapacity int

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

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage. Use a connection pool to manage access
// to multiple database connections from multiple goroutines.
type Conn struct {
	pgConn             *pgconn.PgConn
	config             *ConnConfig // config used when establishing this connection
	preparedStatements map[string]*pgconn.StatementDescription
	statementCache     stmtcache.Cache
	descriptionCache   stmtcache.Cache
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

var errDisabledStatementCache = fmt.Errorf("cannot use QueryExecModeCacheStatement with disabled statement cache")
var errDisabledDescriptionCache = fmt.Errorf("cannot use QueryExecModeCacheDescribe with disabled description cache")

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
//	default_query_exec_mode
//		Possible values: "cache_statement", "cache_describe", "describe_exec", "exec", and "simple_protocol". See
//		QueryExecMode constant documentation for the meaning of these values. Default: "cache_statement".
//
// 	statement_cache_capacity
// 		The maximum size of the statement cache used when executing a query with "cache_statement" query exec mode.
// 		Default: 512.
//
// 	description_cache_capacity
// 		The maximum size of the description cache used when executing a query with "cache_describe" query exec mode.
// 		Default: 512.
func ParseConfig(connString string) (*ConnConfig, error) {
	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	statementCacheCapacity := 512
	if s, ok := config.RuntimeParams["statement_cache_capacity"]; ok {
		delete(config.RuntimeParams, "statement_cache_capacity")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse statement_cache_capacity: %w", err)
		}
		statementCacheCapacity = int(n)
	}

	descriptionCacheCapacity := 512
	if s, ok := config.RuntimeParams["description_cache_capacity"]; ok {
		delete(config.RuntimeParams, "description_cache_capacity")
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse description_cache_capacity: %w", err)
		}
		descriptionCacheCapacity = int(n)
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
		Config:                   *config,
		createdByParseConfig:     true,
		LogLevel:                 LogLevelInfo,
		StatementCacheCapacity:   statementCacheCapacity,
		DescriptionCacheCapacity: descriptionCacheCapacity,
		DefaultQueryExecMode:     defaultQueryExecMode,
		connString:               connString,
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

	if c.config.StatementCacheCapacity > 0 {
		c.statementCache = stmtcache.New(c.pgConn, stmtcache.ModePrepare, c.config.StatementCacheCapacity)
	}

	if c.config.DescriptionCacheCapacity > 0 {
		c.descriptionCache = stmtcache.New(c.pgConn, stmtcache.ModeDescribe, c.config.DescriptionCacheCapacity)
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
	mode := c.config.DefaultQueryExecMode

optionLoop:
	for len(arguments) > 0 {
		switch arg := arguments[0].(type) {
		case QueryExecMode:
			mode = arg
			arguments = arguments[1:]
		default:
			break optionLoop
		}
	}

	// Always use simple protocol when there are no arguments.
	if len(arguments) == 0 {
		mode = QueryExecModeSimpleProtocol
	}

	if sd, ok := c.preparedStatements[sql]; ok {
		return c.execPrepared(ctx, sd, arguments)
	}

	switch mode {
	case QueryExecModeCacheStatement:
		if c.statementCache == nil {
			return pgconn.CommandTag{}, errDisabledStatementCache
		}
		sd, err := c.statementCache.Get(ctx, sql)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		return c.execPrepared(ctx, sd, arguments)
	case QueryExecModeCacheDescribe:
		if c.descriptionCache == nil {
			return pgconn.CommandTag{}, errDisabledDescriptionCache
		}
		sd, err := c.descriptionCache.Get(ctx, sql)
		if err != nil {
			return pgconn.CommandTag{}, err
		}

		return c.execParams(ctx, sd, arguments)
	case QueryExecModeDescribeExec:
		sd, err := c.Prepare(ctx, "", sql)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		return c.execPrepared(ctx, sd, arguments)
	case QueryExecModeExec:
		return c.execSQLParams(ctx, sql, arguments)
	case QueryExecModeSimpleProtocol:
		return c.execSimpleProtocol(ctx, sql, arguments)
	default:
		return pgconn.CommandTag{}, fmt.Errorf("unknown QueryExecMode: %v", mode)
	}
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

func (c *Conn) execParamsAndPreparedPrefix(sd *pgconn.StatementDescription, args []interface{}) error {
	if len(sd.ParamOIDs) != len(args) {
		return fmt.Errorf("expected %d arguments, got %d", len(sd.ParamOIDs), len(args))
	}

	c.eqb.Reset()

	anynil.NormalizeSlice(args)

	for i := range args {
		err := c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], args[i])
		if err != nil {
			err = fmt.Errorf("failed to encode args[%d]: %v", i, err)
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

type unknownArgumentTypeQueryExecModeExecError struct {
	arg interface{}
}

func (e *unknownArgumentTypeQueryExecModeExecError) Error() string {
	return fmt.Sprintf("cannot use unregistered type %T as query argument in QueryExecModeExec", e.arg)
}

func (c *Conn) execSQLParams(ctx context.Context, sql string, args []interface{}) (pgconn.CommandTag, error) {
	c.eqb.Reset()

	anynil.NormalizeSlice(args)
	err := c.appendParamsForQueryExecModeExec(args)
	if err != nil {
		return pgconn.CommandTag{}, err
	}

	result := c.pgConn.ExecParams(ctx, sql, c.eqb.paramValues, nil, c.eqb.paramFormats, c.eqb.resultFormats).Read()
	c.eqb.Reset() // Allow c.eqb internal memory to be GC'ed as soon as possible.
	return result.CommandTag, result.Err
}

// appendParamsForQueryExecModeExec appends the args to c.eqb.
//
// Parameters must be encoded in the text format because of differences in type conversion between timestamps and
// dates. In QueryExecModeExec we don't know what the actual PostgreSQL type is. To determine the type we use the
// Go type to OID type mapping registered by RegisterDefaultPgType. However, the Go time.Time represents both
// PostgreSQL timestamp[tz] and date. To use the binary format we would need to also specify what the PostgreSQL
// type OID is. But that would mean telling PostgreSQL that we have sent a timestamp[tz] when what is needed is a date.
// This means that the value is converted from text to timestamp[tz] to date. This means it does a time zone conversion
// before converting it to date. This means that dates can be shifted by one day. In text format without that double
// type conversion it takes the date directly and ignores time zone (i.e. it works).
//
// Given that the whole point of QueryExecModeExec is to operate without having to know the PostgreSQL types there is
// no way to safely use binary or to specify the parameter OIDs.
func (c *Conn) appendParamsForQueryExecModeExec(args []interface{}) error {
	for _, arg := range args {
		if arg == nil {
			err := c.eqb.AppendParamFormat(c.typeMap, 0, TextFormatCode, arg)
			if err != nil {
				return err
			}
		} else {
			dt, ok := c.TypeMap().TypeForValue(arg)
			if !ok {
				var tv pgtype.TextValuer
				if tv, ok = arg.(pgtype.TextValuer); ok {
					t, err := tv.TextValue()
					if err != nil {
						return err
					}

					dt, ok = c.TypeMap().TypeForOID(pgtype.TextOID)
					if ok {
						arg = t
					}
				}
			}
			if !ok {
				var str fmt.Stringer
				if str, ok = arg.(fmt.Stringer); ok {
					dt, ok = c.TypeMap().TypeForOID(pgtype.TextOID)
					if ok {
						arg = str.String()
					}
				}
			}
			if !ok {
				return &unknownArgumentTypeQueryExecModeExecError{arg: arg}
			}
			err := c.eqb.AppendParamFormat(c.typeMap, dt.OID, TextFormatCode, arg)
			if err != nil {
				return err
			}
		}
	}

	return nil
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

	// Assume the PostgreSQL query parameter types based on the Go type of the arguments. This uses the extended protocol
	// with text formatted parameters and results. Queries are executed in a single round trip. Type mappings can be
	// registered with pgtype.Map.RegisterDefaultPgType. Queries will be rejected that have arguments that are
	// unregistered or ambigious. e.g. A map[string]string may have the PostgreSQL type json or hstore. Modes that know
	// the PostgreSQL type can use a map[string]string directly as an argument. This mode cannot.
	QueryExecModeExec

	// Use the simple protocol. Assume the PostgreSQL query parameter types based on the Go type of the arguments.
	// Queries are executed in a single round trip. Type mappings can be registered with
	// pgtype.Map.RegisterDefaultPgType. Queries will be rejected that have arguments that are unregistered or ambigious.
	// e.g. A map[string]string may have the PostgreSQL type json or hstore. Modes that know the PostgreSQL type can use
	// a map[string]string directly as an argument. This mode cannot.
	//
	// QueryExecModeSimpleProtocol should have the user application visible behavior as QueryExecModeExec with minor
	// exceptions such as behavior when multiple result returning queries are erroneously sent in a single string.
	//
	// QueryExecModeSimpleProtocol uses client side parameter interpolation. All values are quoted and escaped. Prefer
	// QueryExecModeExec over QueryExecModeSimpleProtocol whenever possible. In general QueryExecModeSimpleProtocol
	// should only be used if connecting to a proxy server, connection pool server, or non-PostgreSQL server that does
	// not support the extended protocol.
	QueryExecModeSimpleProtocol
)

func (m QueryExecMode) String() string {
	switch m {
	case QueryExecModeCacheStatement:
		return "cache statement"
	case QueryExecModeCacheDescribe:
		return "cache describe"
	case QueryExecModeDescribeExec:
		return "describe exec"
	case QueryExecModeExec:
		return "exec"
	case QueryExecModeSimpleProtocol:
		return "simple protocol"
	default:
		return "invalid"
	}
}

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
// For extra control over how the query is executed, the types QueryExecMode, QueryResultFormats, and
// QueryResultFormatsByOID may be used as the first args to control exactly how the query is executed. This is rarely
// needed. See the documentation for those types for details.
func (c *Conn) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	var resultFormats QueryResultFormats
	var resultFormatsByOID QueryResultFormatsByOID
	mode := c.config.DefaultQueryExecMode

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
			mode = arg
			args = args[1:]
		default:
			break optionLoop
		}
	}

	c.eqb.Reset()
	anynil.NormalizeSlice(args)
	rows := c.getRows(ctx, sql, args)

	var err error
	sd := c.preparedStatements[sql]
	if sd != nil || mode == QueryExecModeCacheStatement || mode == QueryExecModeCacheDescribe || mode == QueryExecModeDescribeExec {
		if sd == nil {
			switch mode {
			case QueryExecModeCacheStatement:
				if c.statementCache == nil {
					err = errDisabledStatementCache
					rows.fatal(err)
					return rows, err
				}
				sd, err = c.statementCache.Get(ctx, sql)
				if err != nil {
					rows.fatal(err)
					return rows, err
				}
			case QueryExecModeCacheDescribe:
				if c.descriptionCache == nil {
					err = errDisabledDescriptionCache
					rows.fatal(err)
					return rows, err
				}
				sd, err = c.descriptionCache.Get(ctx, sql)
				if err != nil {
					rows.fatal(err)
					return rows, err
				}
			case QueryExecModeDescribeExec:
				sd, err = c.Prepare(ctx, "", sql)
				if err != nil {
					rows.fatal(err)
					return rows, err
				}
			}
		}

		if len(sd.ParamOIDs) != len(args) {
			rows.fatal(fmt.Errorf("expected %d arguments, got %d", len(sd.ParamOIDs), len(args)))
			return rows, rows.err
		}

		rows.sql = sd.SQL

		for i := range args {
			err = c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], args[i])
			if err != nil {
				err = fmt.Errorf("failed to encode args[%d]: %v", i, err)
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

		if mode == QueryExecModeCacheDescribe {
			rows.resultReader = c.pgConn.ExecParams(ctx, sql, c.eqb.paramValues, sd.ParamOIDs, c.eqb.paramFormats, resultFormats)
		} else {
			rows.resultReader = c.pgConn.ExecPrepared(ctx, sd.Name, c.eqb.paramValues, c.eqb.paramFormats, resultFormats)
		}
	} else if mode == QueryExecModeExec {
		err := c.appendParamsForQueryExecModeExec(args)
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}

		rows.resultReader = c.pgConn.ExecParams(ctx, sql, c.eqb.paramValues, nil, c.eqb.paramFormats, c.eqb.resultFormats)
	} else if mode == QueryExecModeSimpleProtocol {
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
	} else {
		err = fmt.Errorf("unknown QueryExecMode: %v", mode)
		rows.fatal(err)
		return rows, rows.err
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
	mode := c.config.DefaultQueryExecMode

	if mode == QueryExecModeSimpleProtocol {
		var sb strings.Builder
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

	batch := &pgconn.Batch{}

	if mode == QueryExecModeExec {
		for _, bi := range b.items {
			c.eqb.Reset()
			anynil.NormalizeSlice(bi.arguments)

			sd := c.preparedStatements[bi.query]
			if sd != nil {
				if len(sd.ParamOIDs) != len(bi.arguments) {
					return &batchResults{ctx: ctx, conn: c, err: fmt.Errorf("mismatched param and argument count")}
				}

				for i := range bi.arguments {
					err := c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], bi.arguments[i])
					if err != nil {
						err = fmt.Errorf("failed to encode args[%d]: %v", i, err)
						return &batchResults{ctx: ctx, conn: c, err: err}
					}
				}

				for i := range sd.Fields {
					c.eqb.AppendResultFormat(c.TypeMap().FormatCodeForOID(sd.Fields[i].DataTypeOID))
				}

				batch.ExecPrepared(sd.Name, c.eqb.paramValues, c.eqb.paramFormats, c.eqb.resultFormats)
			} else {
				err := c.appendParamsForQueryExecModeExec(bi.arguments)
				if err != nil {
					return &batchResults{ctx: ctx, conn: c, err: err}
				}
				batch.ExecParams(bi.query, c.eqb.paramValues, nil, c.eqb.paramFormats, c.eqb.resultFormats)
			}
		}
	} else {

		distinctUnpreparedQueries := map[string]struct{}{}

		for _, bi := range b.items {
			if _, ok := c.preparedStatements[bi.query]; ok {
				continue
			}
			distinctUnpreparedQueries[bi.query] = struct{}{}
		}

		var stmtCache stmtcache.Cache
		if len(distinctUnpreparedQueries) > 0 {
			if mode == QueryExecModeCacheStatement && c.statementCache != nil && c.statementCache.Cap() >= len(distinctUnpreparedQueries) {
				stmtCache = c.statementCache
			} else if mode == QueryExecModeCacheStatement && c.descriptionCache != nil && c.descriptionCache.Cap() >= len(distinctUnpreparedQueries) {
				stmtCache = c.descriptionCache
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

			anynil.NormalizeSlice(bi.arguments)

			for i := range bi.arguments {
				err := c.eqb.AppendParam(c.typeMap, sd.ParamOIDs[i], bi.arguments[i])
				if err != nil {
					err = fmt.Errorf("failed to encode args[%d]: %v", i, err)
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
