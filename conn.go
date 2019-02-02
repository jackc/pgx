package pgx

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/jackc/pgx/pgconn"
	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
)

const (
	connStatusUninitialized = iota
	connStatusClosed
	connStatusIdle
	connStatusBusy
)

// minimalConnInfo has just enough static type information to establish the
// connection and retrieve the type data.
var minimalConnInfo *pgtype.ConnInfo

func init() {
	minimalConnInfo = pgtype.NewConnInfo()
	minimalConnInfo.InitializeDataTypes(map[string]pgtype.OID{
		"int4":    pgtype.Int4OID,
		"name":    pgtype.NameOID,
		"oid":     pgtype.OIDOID,
		"text":    pgtype.TextOID,
		"varchar": pgtype.VarcharOID,
	})
}

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	pgconn.Config
	Logger         Logger
	LogLevel       int
	CustomConnInfo func(*Conn) (*pgtype.ConnInfo, error) // Callback function to implement connection strategies for different backends. crate, pgbouncer, pgpool, etc.
	CustomCancel   func(*Conn) error                     // Callback function used to override cancellation behavior

	// PreferSimpleProtocol disables implicit prepared statement usage. By default
	// pgx automatically uses the unnamed prepared statement for Query and
	// QueryRow. It also uses a prepared statement when Exec has arguments. This
	// can improve performance due to being able to use the binary format. It also
	// does not rely on client side parameter sanitization. However, it does incur
	// two round-trips per query and may be incompatible proxies such as
	// PGBouncer. Setting PreferSimpleProtocol causes the simple protocol to be
	// used by default. The same functionality can be controlled on a per query
	// basis by setting QueryExOptions.SimpleProtocol.
	PreferSimpleProtocol bool
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	pgConn             *pgconn.PgConn
	wbuf               []byte
	config             *ConnConfig // config used when establishing this connection
	preparedStatements map[string]*PreparedStatement
	channels           map[string]struct{}
	logger             Logger
	logLevel           int
	fp                 *fastpath
	poolResetCount     int
	preallocatedRows   []Rows

	mux          sync.Mutex
	status       byte // One of connStatus* constants
	causeOfDeath error

	pendingReadyForQueryCount int // number of ReadyForQuery messages expected
	cancelQueryCompleted      chan struct{}
	lastStmtSent              bool

	// context support
	ctxInProgress bool
	doneChan      chan struct{}
	closedChan    chan error

	ConnInfo *pgtype.ConnInfo
}

// PreparedStatement is a description of a prepared statement
type PreparedStatement struct {
	Name              string
	SQL               string
	FieldDescriptions []FieldDescription
	ParameterOIDs     []pgtype.OID
}

// PrepareExOptions is an option struct that can be passed to PrepareEx
type PrepareExOptions struct {
	ParameterOIDs []pgtype.OID
}

// Identifier a PostgreSQL identifier or name. Identifiers can be composed of
// multiple parts such as ["schema", "table"] or ["table", "column"].
type Identifier []string

// Sanitize returns a sanitized string safe for SQL interpolation.
func (ident Identifier) Sanitize() string {
	parts := make([]string, len(ident))
	for i := range ident {
		parts[i] = `"` + strings.Replace(ident[i], `"`, `""`, -1) + `"`
	}
	return strings.Join(parts, ".")
}

// ErrNoRows occurs when rows are expected but none are returned.
var ErrNoRows = errors.New("no rows in result set")

// ErrDeadConn occurs on an attempt to use a dead connection
var ErrDeadConn = errors.New("conn is dead")

// ErrTLSRefused occurs when the connection attempt requires TLS and the
// PostgreSQL server refuses to use TLS
var ErrTLSRefused = pgconn.ErrTLSRefused

// ErrConnBusy occurs when the connection is busy (for example, in the middle of
// reading query results) and another action is attempted.
var ErrConnBusy = errors.New("conn is busy")

// ErrInvalidLogLevel occurs on attempt to set an invalid log level.
var ErrInvalidLogLevel = errors.New("invalid log level")

// ProtocolError occurs when unexpected data is received from PostgreSQL
type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}

// Connect establishes a connection with a PostgreSQL server with a connection string. See
// pgconn.Connect for details.
func Connect(ctx context.Context, connString string) (*Conn, error) {
	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	connConfig := &ConnConfig{
		Config: *config,
	}

	return connect(ctx, connConfig, minimalConnInfo)
}

// Connect establishes a connection with a PostgreSQL server with a configuration struct.
func ConnectConfig(ctx context.Context, connConfig *ConnConfig) (*Conn, error) {
	return connect(ctx, connConfig, minimalConnInfo)
}

func defaultDialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 5 * time.Minute}
}

func connect(ctx context.Context, config *ConnConfig, connInfo *pgtype.ConnInfo) (c *Conn, err error) {
	c = new(Conn)

	c.config = config
	c.ConnInfo = connInfo

	if c.config.LogLevel != 0 {
		c.logLevel = c.config.LogLevel
	} else {
		// Preserve pre-LogLevel behavior by defaulting to LogLevelDebug
		c.logLevel = LogLevelDebug
	}
	c.logger = c.config.Logger

	if c.shouldLog(LogLevelInfo) {
		c.log(LogLevelInfo, "Dialing PostgreSQL server", map[string]interface{}{"host": config.Config.Host})
	}
	c.pgConn, err = pgconn.ConnectConfig(ctx, &config.Config)
	if err != nil {
		return nil, err
	}

	if err != nil {
		if c.shouldLog(LogLevelError) {
			c.log(LogLevelError, "connect failed", map[string]interface{}{"err": err})
		}
		return nil, err
	}

	c.preparedStatements = make(map[string]*PreparedStatement)
	c.channels = make(map[string]struct{})
	c.cancelQueryCompleted = make(chan struct{})
	close(c.cancelQueryCompleted)
	c.doneChan = make(chan struct{})
	c.closedChan = make(chan error)
	c.wbuf = make([]byte, 0, 1024)
	c.status = connStatusIdle

	// Replication connections can't execute the queries to
	// populate the c.PgTypes and c.pgsqlAfInet
	if _, ok := c.pgConn.Config.RuntimeParams["replication"]; ok {
		return c, nil
	}

	if c.ConnInfo == minimalConnInfo {
		err = c.initConnInfo()
		if err != nil {
			c.Close()
			return nil, err
		}
	}

	return c, nil
}

func initPostgresql(c *Conn) (*pgtype.ConnInfo, error) {
	const (
		namedOIDQuery = `select t.oid,
	case when nsp.nspname in ('pg_catalog', 'public') then t.typname
		else nsp.nspname||'.'||t.typname
	end
from pg_type t
left join pg_type base_type on t.typelem=base_type.oid
left join pg_namespace nsp on t.typnamespace=nsp.oid
where (
	  t.typtype in('b', 'p', 'r', 'e')
	  and (base_type.oid is null or base_type.typtype in('b', 'p', 'r'))
	)`
	)

	nameOIDs, err := connInfoFromRows(c.Query(namedOIDQuery))
	if err != nil {
		return nil, err
	}

	cinfo := pgtype.NewConnInfo()
	cinfo.InitializeDataTypes(nameOIDs)

	if err = c.initConnInfoEnumArray(cinfo); err != nil {
		return nil, err
	}

	if err = c.initConnInfoDomains(cinfo); err != nil {
		return nil, err
	}

	return cinfo, nil
}

func (c *Conn) initConnInfo() (err error) {
	var (
		connInfo *pgtype.ConnInfo
	)

	if c.config.CustomConnInfo != nil {
		if c.ConnInfo, err = c.config.CustomConnInfo(c); err != nil {
			return err
		}

		return nil
	}

	if connInfo, err = initPostgresql(c); err == nil {
		c.ConnInfo = connInfo
		return err
	}

	// Check if CrateDB specific approach might still allow us to connect.
	if connInfo, err = c.crateDBTypesQuery(err); err == nil {
		c.ConnInfo = connInfo
	}

	return err
}

// initConnInfoEnumArray introspects for arrays of enums and registers a data type for them.
func (c *Conn) initConnInfoEnumArray(cinfo *pgtype.ConnInfo) error {
	nameOIDs := make(map[string]pgtype.OID, 16)
	rows, err := c.Query(`select t.oid, t.typname
from pg_type t
  join pg_type base_type on t.typelem=base_type.oid
where t.typtype = 'b'
  and base_type.typtype = 'e'`)
	if err != nil {
		return err
	}

	for rows.Next() {
		var oid pgtype.OID
		var name pgtype.Text
		if err := rows.Scan(&oid, &name); err != nil {
			return err
		}

		nameOIDs[name.String] = oid
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	for name, oid := range nameOIDs {
		cinfo.RegisterDataType(pgtype.DataType{
			Value: &pgtype.EnumArray{},
			Name:  name,
			OID:   oid,
		})
	}

	return nil
}

// initConnInfoDomains introspects for domains and registers a data type for them.
func (c *Conn) initConnInfoDomains(cinfo *pgtype.ConnInfo) error {
	type domain struct {
		oid     pgtype.OID
		name    pgtype.Text
		baseOID pgtype.OID
	}

	domains := make([]*domain, 0, 16)

	rows, err := c.Query(`select t.oid, t.typname, t.typbasetype
from pg_type t
  join pg_type base_type on t.typbasetype=base_type.oid
where t.typtype = 'd'
  and base_type.typtype = 'b'`)
	if err != nil {
		return err
	}

	for rows.Next() {
		var d domain
		if err := rows.Scan(&d.oid, &d.name, &d.baseOID); err != nil {
			return err
		}

		domains = append(domains, &d)
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	for _, d := range domains {
		baseDataType, ok := cinfo.DataTypeForOID(d.baseOID)
		if ok {
			cinfo.RegisterDataType(pgtype.DataType{
				Value: reflect.New(reflect.ValueOf(baseDataType.Value).Elem().Type()).Interface().(pgtype.Value),
				Name:  d.name.String,
				OID:   d.oid,
			})
		}
	}

	return nil
}

// crateDBTypesQuery checks if the given err is likely to be the result of
// CrateDB not implementing the pg_types table correctly. If yes, a CrateDB
// specific query against pg_types is executed and its results are returned. If
// not, the original error is returned.
func (c *Conn) crateDBTypesQuery(err error) (*pgtype.ConnInfo, error) {
	// CrateDB 2.1.6 is a database that implements the PostgreSQL wire protocol,
	// but not perfectly. In particular, the pg_catalog schema containing the
	// pg_type table is not visible by default and the pg_type.typtype column is
	// not implemented. Therefor the query above currently returns the following
	// error:
	//
	//   pgx.PgError{Severity:"ERROR", Code:"XX000",
	//   Message:"TableUnknownException: Table 'test.pg_type' unknown",
	//   Detail:"", Hint:"", Position:0, InternalPosition:0, InternalQuery:"",
	//   Where:"", SchemaName:"", TableName:"", ColumnName:"", DataTypeName:"",
	//   ConstraintName:"", File:"Schemas.java", Line:99, Routine:"getTableInfo"}
	//
	// If CrateDB was to fix the pg_type table visbility in the future, we'd
	// still get this error until typtype column is implemented:
	//
	//   pgx.PgError{Severity:"ERROR", Code:"XX000",
	//   Message:"ColumnUnknownException: Column typtype unknown", Detail:"",
	//   Hint:"", Position:0, InternalPosition:0, InternalQuery:"", Where:"",
	//   SchemaName:"", TableName:"", ColumnName:"", DataTypeName:"",
	//   ConstraintName:"", File:"FullQualifiedNameFieldProvider.java", Line:132,
	//
	// Additionally CrateDB doesn't implement Postgres error codes [2], and
	// instead always returns "XX000" (internal_error). The code below uses all
	// of this knowledge as a heuristic to detect CrateDB. If CrateDB is
	// detected, a CrateDB specific pg_type query is executed instead.
	//
	// The heuristic is designed to still work even if CrateDB fixes [2] or
	// renames its internal exception names. If both are changed but pg_types
	// isn't fixed, this code will need to be changed.
	//
	// There is also a small chance the heuristic will yield a false positive for
	// non-CrateDB databases (e.g. if a real Postgres instance returns a XX000
	// error), but hopefully there will be no harm in attempting the alternative
	// query in this case.
	//
	// CrateDB also uses the type varchar for the typname column which required
	// adding varchar to the minimalConnInfo init code.
	//
	// Also see the discussion here [3].
	//
	// [1] https://crate.io/
	// [2] https://github.com/crate/crate/issues/5027
	// [3] https://github.com/jackc/pgx/issues/320

	if pgErr, ok := err.(*pgconn.PgError); ok &&
		(pgErr.Code == "XX000" ||
			strings.Contains(pgErr.Message, "TableUnknownException") ||
			strings.Contains(pgErr.Message, "ColumnUnknownException")) {
		var (
			nameOIDs map[string]pgtype.OID
		)

		if nameOIDs, err = connInfoFromRows(c.Query(`select oid, typname from pg_catalog.pg_type`)); err != nil {
			return nil, err
		}

		cinfo := pgtype.NewConnInfo()
		cinfo.InitializeDataTypes(nameOIDs)

		return cinfo, err
	}

	return nil, err
}

// PID returns the backend PID for this connection.
func (c *Conn) PID() uint32 {
	return c.pgConn.PID()
}

// LocalAddr returns the underlying connection's local address
func (c *Conn) LocalAddr() (net.Addr, error) {
	if !c.IsAlive() {
		return nil, errors.New("connection not ready")
	}
	return c.pgConn.Conn().LocalAddr(), nil
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.status < connStatusIdle {
		return nil
	}
	c.status = connStatusClosed

	err := c.pgConn.Close(context.TODO())
	c.causeOfDeath = errors.New("Closed")
	if c.shouldLog(LogLevelInfo) {
		c.log(LogLevelInfo, "closed connection", nil)
	}
	return err
}

// ParameterStatus returns the value of a parameter reported by the server (e.g.
// server_version). Returns an empty string for unknown parameters.
func (c *Conn) ParameterStatus(key string) string {
	return c.pgConn.ParameterStatus(key)
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
//
// Prepare is idempotent; i.e. it is safe to call Prepare multiple times with the same
// name and sql arguments. This allows a code path to Prepare and Query/Exec without
// concern for if the statement has already been prepared.
func (c *Conn) Prepare(name, sql string) (ps *PreparedStatement, err error) {
	return c.PrepareEx(context.Background(), name, sql, nil)
}

// PrepareEx creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
// It differs from Prepare as it allows additional options (such as parameter OIDs) to be passed via struct
//
// PrepareEx is idempotent; i.e. it is safe to call PrepareEx multiple times with the same
// name and sql arguments. This allows a code path to PrepareEx and Query/Exec without
// concern for if the statement has already been prepared.
func (c *Conn) PrepareEx(ctx context.Context, name, sql string, opts *PrepareExOptions) (ps *PreparedStatement, err error) {
	err = c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return nil, err
	}

	if name != "" {
		if ps, ok := c.preparedStatements[name]; ok && ps.SQL == sql {
			return ps, nil
		}
	}

	if err := c.ensureConnectionReadyForQuery(); err != nil {
		return nil, err
	}

	if c.shouldLog(LogLevelError) {
		defer func() {
			if err != nil {
				c.log(LogLevelError, "prepareEx failed", map[string]interface{}{"err": err, "name": name, "sql": sql})
			}
		}()
	}

	if opts == nil {
		opts = &PrepareExOptions{}
	}

	if len(opts.ParameterOIDs) > 65535 {
		return nil, errors.Errorf("Number of PrepareExOptions ParameterOIDs must be between 0 and 65535, received %d", len(opts.ParameterOIDs))
	}

	var paramOIDs []uint32
	for _, oid := range opts.ParameterOIDs {
		paramOIDs = append(paramOIDs, uint32(oid))
	}

	psd, err := c.pgConn.Prepare(context.TODO(), name, sql, paramOIDs)
	if err != nil {
		return nil, err
	}

	ps = &PreparedStatement{
		Name:              psd.Name,
		SQL:               psd.SQL,
		ParameterOIDs:     make([]pgtype.OID, len(psd.ParamOIDs)),
		FieldDescriptions: make([]FieldDescription, len(psd.Fields)),
	}

	for i := range ps.ParameterOIDs {
		ps.ParameterOIDs[i] = pgtype.OID(psd.ParamOIDs[i])
	}
	for i := range ps.FieldDescriptions {
		c.pgproto3FieldDescriptionToPgxFieldDescription(&psd.Fields[i], &ps.FieldDescriptions[i])
	}

	if name != "" {
		c.preparedStatements[name] = ps
	}

	return ps, nil
}

// Deallocate released a prepared statement
func (c *Conn) Deallocate(name string) error {
	return c.deallocateContext(context.Background(), name)
}

// TODO - consider making this public
func (c *Conn) deallocateContext(ctx context.Context, name string) (err error) {
	err = c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return err
	}

	err = c.initContext(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = c.termContext(err)
	}()

	if err := c.ensureConnectionReadyForQuery(); err != nil {
		return err
	}

	delete(c.preparedStatements, name)

	_, err = c.pgConn.Exec(ctx, "deallocate "+quoteIdentifier(name)).ReadAll()
	return err
}

// Listen establishes a PostgreSQL listen/notify to channel
func (c *Conn) Listen(channel string) error {
	_, err := c.Exec(context.TODO(), "listen "+quoteIdentifier(channel))
	if err != nil {
		return err
	}

	c.channels[channel] = struct{}{}

	return nil
}

// Unlisten unsubscribes from a listen channel
func (c *Conn) Unlisten(channel string) error {
	_, err := c.Exec(context.TODO(), "unlisten "+quoteIdentifier(channel))
	if err != nil {
		return err
	}

	delete(c.channels, channel)
	return nil
}

func (c *Conn) IsAlive() bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.pgConn.IsAlive() && c.status >= connStatusIdle
}

func (c *Conn) CauseOfDeath() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.causeOfDeath
}

// fatalWriteError takes the response of a net.Conn.Write and determines if it is fatal
func fatalWriteErr(bytesWritten int, err error) bool {
	// Partial writes break the connection
	if bytesWritten > 0 {
		return true
	}

	netErr, is := err.(net.Error)
	return !(is && netErr.Timeout())
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages is the same
// regardless of when they occur. It also ignores messages that are only
// meaningful in a given context. These messages can occur due to a context
// deadline interrupting message processing. For example, an interrupted query
// may have left DataRow messages on the wire.
func (c *Conn) processContextFreeMsg(msg pgproto3.BackendMessage) (err error) {
	switch msg := msg.(type) {
	case *pgproto3.ErrorResponse:
		return c.rxErrorResponse(msg)
	case *pgproto3.ReadyForQuery:
		c.rxReadyForQuery(msg)
	}

	return nil
}

func (c *Conn) rxErrorResponse(msg *pgproto3.ErrorResponse) *pgconn.PgError {
	err := &pgconn.PgError{
		Severity:         msg.Severity,
		Code:             msg.Code,
		Message:          msg.Message,
		Detail:           msg.Detail,
		Hint:             msg.Hint,
		Position:         msg.Position,
		InternalPosition: msg.InternalPosition,
		InternalQuery:    msg.InternalQuery,
		Where:            msg.Where,
		SchemaName:       msg.SchemaName,
		TableName:        msg.TableName,
		ColumnName:       msg.ColumnName,
		DataTypeName:     msg.DataTypeName,
		ConstraintName:   msg.ConstraintName,
		File:             msg.File,
		Line:             msg.Line,
		Routine:          msg.Routine,
	}

	if err.Severity == "FATAL" {
		c.die(err)
	}

	return err
}

func (c *Conn) rxReadyForQuery(msg *pgproto3.ReadyForQuery) {
	c.pendingReadyForQueryCount--
}

func (c *Conn) rxRowDescription(msg *pgproto3.RowDescription) []FieldDescription {
	fields := make([]FieldDescription, len(msg.Fields))
	for i := 0; i < len(fields); i++ {
		fields[i].Name = string(msg.Fields[i].Name)
		fields[i].Table = pgtype.OID(msg.Fields[i].TableOID)
		fields[i].AttributeNumber = msg.Fields[i].TableAttributeNumber
		fields[i].DataType = pgtype.OID(msg.Fields[i].DataTypeOID)
		fields[i].DataTypeSize = msg.Fields[i].DataTypeSize
		fields[i].Modifier = msg.Fields[i].TypeModifier
		fields[i].FormatCode = msg.Fields[i].Format
	}
	return fields
}

func (c *Conn) rxParameterDescription(msg *pgproto3.ParameterDescription) []pgtype.OID {
	parameters := make([]pgtype.OID, len(msg.ParameterOIDs))
	for i := 0; i < len(parameters); i++ {
		parameters[i] = pgtype.OID(msg.ParameterOIDs[i])
	}
	return parameters
}

func (c *Conn) die(err error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.status == connStatusClosed {
		return
	}

	c.status = connStatusClosed
	c.causeOfDeath = err
	c.pgConn.Conn().Close()
}

func (c *Conn) lock() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.status != connStatusIdle {
		return ErrConnBusy
	}

	c.status = connStatusBusy
	return nil
}

func (c *Conn) unlock() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.status != connStatusBusy {
		return errors.New("unlock conn that is not busy")
	}

	c.status = connStatusIdle
	return nil
}

func (c *Conn) shouldLog(lvl int) bool {
	return c.logger != nil && c.logLevel >= lvl
}

func (c *Conn) log(lvl LogLevel, msg string, data map[string]interface{}) {
	if data == nil {
		data = map[string]interface{}{}
	}
	if c.pgConn != nil && c.pgConn.PID() != 0 {
		data["pid"] = c.pgConn.PID()
	}

	c.logger.Log(lvl, msg, data)
}

// SetLogger replaces the current logger and returns the previous logger.
func (c *Conn) SetLogger(logger Logger) Logger {
	oldLogger := c.logger
	c.logger = logger
	return oldLogger
}

// SetLogLevel replaces the current log level and returns the previous log
// level.
func (c *Conn) SetLogLevel(lvl int) (int, error) {
	oldLvl := c.logLevel

	if lvl < LogLevelNone || lvl > LogLevelTrace {
		return oldLvl, ErrInvalidLogLevel
	}

	c.logLevel = lvl
	return lvl, nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.Replace(s, `"`, `""`, -1) + `"`
}

func doCancel(c *Conn) error {
	// ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	// defer cancel()
	// return c.pgConn.CancelRequest(ctx)
	return errors.New("TODO - reimplement cancellation")
}

// cancelQuery sends a cancel request to the PostgreSQL server. It returns an
// error if unable to deliver the cancel request, but lack of an error does not
// ensure that the query was canceled. As specified in the documentation, there
// is no way to be sure a query was canceled. See
// https://www.postgresql.org/docs/current/static/protocol-flow.html#AEN112861
func (c *Conn) cancelQuery() {
	if err := c.pgConn.Conn().SetDeadline(time.Now()); err != nil {
		c.Close() // Close connection if unable to set deadline
		return
	}

	var cancelFn func(*Conn) error
	completeCh := make(chan struct{})
	c.mux.Lock()
	c.cancelQueryCompleted = completeCh
	c.mux.Unlock()
	if c.config.CustomCancel != nil {
		cancelFn = c.config.CustomCancel
	} else {
		cancelFn = doCancel
	}

	go func() {
		defer close(completeCh)
		err := cancelFn(c)
		if err != nil {
			c.Close() // Something is very wrong. Terminate the connection.
		}
	}()
}

func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.Exec(ctx, ";", nil)
	return err
}

func (c *Conn) initContext(ctx context.Context) error {
	if c.ctxInProgress {
		return errors.New("ctx already in progress")
	}

	if ctx.Done() == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.ctxInProgress = true

	go c.contextHandler(ctx)

	return nil
}

func (c *Conn) termContext(opErr error) error {
	if !c.ctxInProgress {
		return opErr
	}

	var err error

	select {
	case err = <-c.closedChan:
		if opErr == nil {
			err = nil
		}
	case c.doneChan <- struct{}{}:
		err = opErr
	}

	c.ctxInProgress = false
	return err
}

func (c *Conn) contextHandler(ctx context.Context) {
	select {
	case <-ctx.Done():
		c.cancelQuery()
		c.closedChan <- ctx.Err()
	case <-c.doneChan:
	}
}

// WaitUntilReady will return when the connection is ready for another query
func (c *Conn) WaitUntilReady(ctx context.Context) error {
	err := c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return err
	}
	return c.ensureConnectionReadyForQuery()
}

func (c *Conn) waitForPreviousCancelQuery(ctx context.Context) error {
	c.mux.Lock()
	completeCh := c.cancelQueryCompleted
	c.mux.Unlock()
	select {
	case <-completeCh:
		if err := c.pgConn.Conn().SetDeadline(time.Time{}); err != nil {
			c.Close() // Close connection if unable to disable deadline
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) ensureConnectionReadyForQuery() error {
	for c.pendingReadyForQueryCount > 0 {
		msg, err := c.pgConn.ReceiveMessage()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ErrorResponse:
			pgErr := c.rxErrorResponse(msg)
			if pgErr.Severity == "FATAL" {
				return pgErr
			}
		default:
			err = c.processContextFreeMsg(msg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func connInfoFromRows(rows *Rows, err error) (map[string]pgtype.OID, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nameOIDs := make(map[string]pgtype.OID, 256)
	for rows.Next() {
		var oid pgtype.OID
		var name pgtype.Text
		if err = rows.Scan(&oid, &name); err != nil {
			return nil, err
		}

		nameOIDs[name.String] = oid
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return nameOIDs, err
}

// LastStmtSent returns true if the last call to Query(Ex)/Exec(Ex) attempted to
// send the statement over the wire. Each call to a Query(Ex)/Exec(Ex) resets
// the value to false initially until the statement has been sent. This does
// NOT mean that the statement was successful or even received, it just means
// that a write was attempted and therefore it could have been executed. Calls
// to prepare a statement are ignored, only when the prepared statement is
// attempted to be executed will this return true.
func (c *Conn) LastStmtSent() bool {
	return c.lastStmtSent
}

// Exec executes sql. sql can be either a prepared statement name or an SQL string. arguments should be referenced
// positionally from the sql string as $1, $2, etc.
func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	c.lastStmtSent = false
	err := c.waitForPreviousCancelQuery(ctx)
	if err != nil {
		return "", err
	}

	if err := c.lock(); err != nil {
		return "", err
	}
	defer c.unlock()

	if err := c.ensureConnectionReadyForQuery(); err != nil {
		return "", err
	}

	startTime := time.Now()

	commandTag, err := c.exec(ctx, sql, arguments...)
	if err != nil {
		if c.shouldLog(LogLevelError) {
			c.log(LogLevelError, "Exec", map[string]interface{}{"sql": sql, "args": logQueryArgs(arguments), "err": err})
		}
		return commandTag, err
	}

	if c.shouldLog(LogLevelInfo) {
		endTime := time.Now()
		c.log(LogLevelInfo, "Exec", map[string]interface{}{"sql": sql, "args": logQueryArgs(arguments), "time": endTime.Sub(startTime), "commandTag": commandTag})
	}

	return commandTag, err
}

func (c *Conn) exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	if len(arguments) == 0 {
		c.lastStmtSent = true
		results, err := c.pgConn.Exec(ctx, sql).ReadAll()
		if err != nil {
			return "", err
		}
		if len(results) == 0 {
			return "", nil
		}

		return results[len(results)-1].CommandTag, nil
	} else {
		psd, err := c.pgConn.Prepare(ctx, "", sql, nil)
		if err != nil {
			return "", err
		}

		if len(psd.ParamOIDs) != len(arguments) {
			return "", errors.Errorf("expected %d arguments, got %d", len(psd.ParamOIDs), len(arguments))
		}

		ps := &PreparedStatement{
			Name:              psd.Name,
			SQL:               psd.SQL,
			ParameterOIDs:     make([]pgtype.OID, len(psd.ParamOIDs)),
			FieldDescriptions: make([]FieldDescription, len(psd.Fields)),
		}

		for i := range ps.ParameterOIDs {
			ps.ParameterOIDs[i] = pgtype.OID(psd.ParamOIDs[i])
		}
		for i := range ps.FieldDescriptions {
			c.pgproto3FieldDescriptionToPgxFieldDescription(&psd.Fields[i], &ps.FieldDescriptions[i])
		}

		arguments, err = convertDriverValuers(arguments)
		if err != nil {
			return "", err
		}

		paramFormats := make([]int16, len(arguments))
		paramValues := make([][]byte, len(arguments))
		for i := range arguments {
			paramFormats[i] = chooseParameterFormatCode(c.ConnInfo, ps.ParameterOIDs[i], arguments[i])
			paramValues[i], err = newencodePreparedStatementArgument(c.ConnInfo, ps.ParameterOIDs[i], arguments[i])
			if err != nil {
				return "", err
			}

		}

		resultFormats := make([]int16, len(ps.FieldDescriptions))
		for i := range resultFormats {
			if dt, ok := c.ConnInfo.DataTypeForOID(ps.FieldDescriptions[i].DataType); ok {
				if _, ok := dt.Value.(pgtype.BinaryDecoder); ok {
					resultFormats[i] = BinaryFormatCode
				} else {
					resultFormats[i] = TextFormatCode
				}
			}
		}

		c.lastStmtSent = true
		result := c.pgConn.ExecPrepared(ctx, psd.Name, paramValues, paramFormats, resultFormats).Read()
		return result.CommandTag, result.Err
	}

}

func newencodePreparedStatementArgument(ci *pgtype.ConnInfo, oid pgtype.OID, arg interface{}) ([]byte, error) {
	if arg == nil {
		return nil, nil
	}

	switch arg := arg.(type) {
	case pgtype.BinaryEncoder:
		return arg.EncodeBinary(ci, nil)
	case pgtype.TextEncoder:
		return arg.EncodeText(ci, nil)
	case string:
		return []byte(arg), nil
	}

	refVal := reflect.ValueOf(arg)

	if refVal.Kind() == reflect.Ptr {
		if refVal.IsNil() {
			return nil, nil
		}
		arg = refVal.Elem().Interface()
		return newencodePreparedStatementArgument(ci, oid, arg)
	}

	if dt, ok := ci.DataTypeForOID(oid); ok {
		value := dt.Value
		err := value.Set(arg)
		if err != nil {
			{
				if arg, ok := arg.(driver.Valuer); ok {
					v, err := callValuerValue(arg)
					if err != nil {
						return nil, err
					}
					return newencodePreparedStatementArgument(ci, oid, v)
				}
			}

			return nil, err
		}

		return value.(pgtype.BinaryEncoder).EncodeBinary(ci, nil)
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		return newencodePreparedStatementArgument(ci, oid, strippedArg)
	}
	return nil, SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
}

// pgproto3FieldDescriptionToPgxFieldDescription copies and converts the data from a pgproto3.FieldDescription to a
// FieldDescription.
func (c *Conn) pgproto3FieldDescriptionToPgxFieldDescription(src *pgproto3.FieldDescription, dst *FieldDescription) {
	dst.Name = src.Name
	dst.Table = pgtype.OID(src.TableOID)
	dst.AttributeNumber = src.TableAttributeNumber
	dst.DataType = pgtype.OID(src.DataTypeOID)
	dst.DataTypeSize = src.DataTypeSize
	dst.Modifier = src.TypeModifier
	dst.FormatCode = src.Format

	if dt, ok := c.ConnInfo.DataTypeForOID(dst.DataType); ok {
		dst.DataTypeName = dt.Name
	}
}
