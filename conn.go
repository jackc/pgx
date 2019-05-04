package pgx

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

const (
	connStatusUninitialized = iota
	connStatusClosed
	connStatusIdle
	connStatusBusy
)

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	pgconn.Config
	Logger   Logger
	LogLevel LogLevel
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	pgConn             *pgconn.PgConn
	config             *ConnConfig // config used when establishing this connection
	preparedStatements map[string]*PreparedStatement
	logger             Logger
	logLevel           LogLevel
	fp                 *fastpath

	causeOfDeath error

	doneChan   chan struct{}
	closedChan chan error

	ConnInfo *pgtype.ConnInfo

	wbuf             []byte
	preallocatedRows []connRows
	paramFormats     []int16
	paramValues      [][]byte
	resultFormats    []int16
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
	connConfig, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return connect(ctx, connConfig)
}

// Connect establishes a connection with a PostgreSQL server with a configuration struct.
func ConnectConfig(ctx context.Context, connConfig *ConnConfig) (*Conn, error) {
	return connect(ctx, connConfig)
}

func ParseConfig(connString string) (*ConnConfig, error) {
	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	connConfig := &ConnConfig{
		Config: *config,
	}

	return connConfig, nil
}

func connect(ctx context.Context, config *ConnConfig) (c *Conn, err error) {
	c = new(Conn)

	c.config = config
	c.ConnInfo = pgtype.NewConnInfo()

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
	c.doneChan = make(chan struct{})
	c.closedChan = make(chan error)
	c.wbuf = make([]byte, 0, 1024)
	c.paramFormats = make([]int16, 0, 16)
	c.paramValues = make([][]byte, 0, 16)
	c.resultFormats = make([]int16, 0, 32)

	// Replication connections can't execute the queries to
	// populate the c.PgTypes and c.pgsqlAfInet
	if _, ok := c.pgConn.Config.RuntimeParams["replication"]; ok {
		return c, nil
	}

	return c, nil
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close(ctx context.Context) error {
	if !c.IsAlive() {
		return nil
	}

	err := c.pgConn.Close(ctx)
	c.causeOfDeath = errors.New("Closed")
	if c.shouldLog(LogLevelInfo) {
		c.log(LogLevelInfo, "closed connection", nil)
	}
	return err
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
//
// Prepare is idempotent; i.e. it is safe to call Prepare multiple times with the same
// name and sql arguments. This allows a code path to Prepare and Query/Exec without
// concern for if the statement has already been prepared.
func (c *Conn) Prepare(ctx context.Context, name, sql string) (ps *PreparedStatement, err error) {
	if name != "" {
		if ps, ok := c.preparedStatements[name]; ok && ps.SQL == sql {
			return ps, nil
		}
	}

	if c.shouldLog(LogLevelError) {
		defer func() {
			if err != nil {
				c.log(LogLevelError, "Prepare failed", map[string]interface{}{"err": err, "name": name, "sql": sql})
			}
		}()
	}

	psd, err := c.pgConn.Prepare(ctx, name, sql, nil)
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
		pgproto3FieldDescriptionToPgxFieldDescription(c.ConnInfo, &psd.Fields[i], &ps.FieldDescriptions[i])
	}

	if name != "" {
		c.preparedStatements[name] = ps
	}

	return ps, nil
}

// Deallocate released a prepared statement
func (c *Conn) Deallocate(ctx context.Context, name string) error {
	delete(c.preparedStatements, name)
	_, err := c.pgConn.Exec(ctx, "deallocate "+quoteIdentifier(name)).ReadAll()
	return err
}

func (c *Conn) IsAlive() bool {
	return c.pgConn.IsAlive()
}

func (c *Conn) CauseOfDeath() error {
	return c.causeOfDeath
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

func (c *Conn) die(err error) {
	if !c.IsAlive() {
		return
	}

	c.causeOfDeath = err

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // force immediate hard cancel
	c.pgConn.Close(ctx)
}

func (c *Conn) shouldLog(lvl LogLevel) bool {
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
func (c *Conn) SetLogLevel(lvl LogLevel) (LogLevel, error) {
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

func (c *Conn) Ping(ctx context.Context) error {
	_, err := c.Exec(ctx, ";")
	return err
}

func connInfoFromRows(rows Rows, err error) (map[string]pgtype.OID, error) {
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

// PgConn returns the underlying *pgconn.PgConn. This is an escape hatch method that allows lower level access to the
// PostgreSQL connection than pgx exposes.
//
// It is strongly recommended that the connection be idle (no in-progress queries) before the underlying *pgconn.PgConn
// is used and the connection must be returned to the same state before any *pgx.Conn methods are again used.
func (c *Conn) PgConn() *pgconn.PgConn { return c.pgConn }

// Exec executes sql. sql can be either a prepared statement name or an SQL string. arguments should be referenced
// positionally from the sql string as $1, $2, etc.
func (c *Conn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
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

	if ps, ok := c.preparedStatements[sql]; ok {
		args, err := convertDriverValuers(arguments)
		if err != nil {
			return nil, err
		}

		paramFormats := make([]int16, len(args))
		paramValues := make([][]byte, len(args))
		for i := range args {
			paramFormats[i] = chooseParameterFormatCode(c.ConnInfo, ps.ParameterOIDs[i], args[i])
			paramValues[i], err = newencodePreparedStatementArgument(c.ConnInfo, ps.ParameterOIDs[i], args[i])
			if err != nil {
				return nil, err
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

		result := c.pgConn.ExecPrepared(ctx, ps.Name, paramValues, paramFormats, resultFormats).Read()
		return result.CommandTag, result.Err
	}

	if len(arguments) == 0 {
		results, err := c.pgConn.Exec(ctx, sql).ReadAll()
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}

		return results[len(results)-1].CommandTag, nil
	} else {
		psd, err := c.pgConn.Prepare(ctx, "", sql, nil)
		if err != nil {
			return nil, err
		}

		if len(psd.ParamOIDs) != len(arguments) {
			return nil, errors.Errorf("expected %d arguments, got %d", len(psd.ParamOIDs), len(arguments))
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
			pgproto3FieldDescriptionToPgxFieldDescription(c.ConnInfo, &psd.Fields[i], &ps.FieldDescriptions[i])
		}

		arguments, err = convertDriverValuers(arguments)
		if err != nil {
			return nil, err
		}

		paramFormats := make([]int16, len(arguments))
		paramValues := make([][]byte, len(arguments))
		for i := range arguments {
			paramFormats[i] = chooseParameterFormatCode(c.ConnInfo, ps.ParameterOIDs[i], arguments[i])
			paramValues[i], err = newencodePreparedStatementArgument(c.ConnInfo, ps.ParameterOIDs[i], arguments[i])
			if err != nil {
				return nil, err
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

		result := c.pgConn.ExecPrepared(ctx, psd.Name, paramValues, paramFormats, resultFormats).Read()
		return result.CommandTag, result.Err
	}

}

func newencodePreparedStatementArgument(ci *pgtype.ConnInfo, oid pgtype.OID, arg interface{}) ([]byte, error) {
	if arg == nil {
		return nil, nil
	}

	// TODO - don't allocate a new buf for each encoded prepared statement. The empty slice is necessary because otherwise empty strings may be encoded as []byte(nil) instead of []byte{}
	buf := make([]byte, 0)

	switch arg := arg.(type) {
	case pgtype.BinaryEncoder:
		return arg.EncodeBinary(ci, buf)
	case pgtype.TextEncoder:
		return arg.EncodeText(ci, buf)
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

		return value.(pgtype.BinaryEncoder).EncodeBinary(ci, buf)
	}

	if strippedArg, ok := stripNamedType(&refVal); ok {
		return newencodePreparedStatementArgument(ci, oid, strippedArg)
	}
	return nil, SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
}

// pgproto3FieldDescriptionToPgxFieldDescription copies and converts the data from a pgproto3.FieldDescription to a
// FieldDescription.
func pgproto3FieldDescriptionToPgxFieldDescription(connInfo *pgtype.ConnInfo, src *pgproto3.FieldDescription, dst *FieldDescription) {
	dst.Name = string(src.Name)
	dst.Table = pgtype.OID(src.TableOID)
	dst.AttributeNumber = src.TableAttributeNumber
	dst.DataType = pgtype.OID(src.DataTypeOID)
	dst.DataTypeSize = src.DataTypeSize
	dst.Modifier = src.TypeModifier
	dst.FormatCode = src.Format

	if dt, ok := connInfo.DataTypeForOID(dst.DataType); ok {
		dst.DataTypeName = dt.Name
	}
}

func (c *Conn) getRows(sql string, args []interface{}) *connRows {
	if len(c.preallocatedRows) == 0 {
		c.preallocatedRows = make([]connRows, 64)
	}

	r := &c.preallocatedRows[len(c.preallocatedRows)-1]
	c.preallocatedRows = c.preallocatedRows[0 : len(c.preallocatedRows)-1]

	r.logger = c
	r.connInfo = c.ConnInfo
	r.startTime = time.Now()
	r.sql = sql
	r.args = args

	return r
}

// QueryResultFormats controls the result format (text=0, binary=1) of a query by result column position.
type QueryResultFormats []int16

// QueryResultFormatsByOID controls the result format (text=0, binary=1) of a query by the result column OID.
type QueryResultFormatsByOID map[pgtype.OID]int16

// Query executes sql with args. If there is an error the returned Rows will be returned in an error state. So it is
// allowed to ignore the error returned from Query and handle it in Rows.
func (c *Conn) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	var resultFormats QueryResultFormats
	var resultFormatsByOID QueryResultFormatsByOID

optionLoop:
	for len(args) > 0 {
		switch arg := args[0].(type) {
		case QueryResultFormats:
			resultFormats = arg
			args = args[1:]
		case QueryResultFormatsByOID:
			resultFormatsByOID = arg
			args = args[1:]
		default:
			break optionLoop
		}
	}

	rows := c.getRows(sql, args)

	ps, ok := c.preparedStatements[sql]
	if !ok {
		psd, err := c.pgConn.Prepare(ctx, "", sql, nil)
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}

		if len(psd.ParamOIDs) != len(args) {
			rows.fatal(errors.Errorf("expected %d arguments, got %d", len(psd.ParamOIDs), len(args)))
			return rows, rows.err
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
			pgproto3FieldDescriptionToPgxFieldDescription(c.ConnInfo, &psd.Fields[i], &ps.FieldDescriptions[i])
		}
	}
	rows.sql = ps.SQL

	var err error
	args, err = convertDriverValuers(args)
	if err != nil {
		rows.fatal(err)
		return rows, rows.err
	}

	var paramFormats []int16
	if len(args) > cap(c.paramFormats) {
		paramFormats = make([]int16, len(args))
	} else {
		paramFormats = c.paramFormats[:len(args)]
	}

	var paramValues [][]byte
	if len(args) > cap(c.paramValues) {
		paramValues = make([][]byte, len(args))
	} else {
		paramValues = c.paramValues[:len(args)]
	}

	for i := range args {
		paramFormats[i] = chooseParameterFormatCode(c.ConnInfo, ps.ParameterOIDs[i], args[i])
		paramValues[i], err = newencodePreparedStatementArgument(c.ConnInfo, ps.ParameterOIDs[i], args[i])
		if err != nil {
			rows.fatal(err)
			return rows, rows.err
		}
	}

	if resultFormatsByOID != nil {
		resultFormats = make([]int16, len(ps.FieldDescriptions))
		for i := range resultFormats {
			resultFormats[i] = resultFormatsByOID[ps.FieldDescriptions[i].DataType]
		}
	}

	if resultFormats == nil {
		if len(ps.FieldDescriptions) > cap(c.resultFormats) {
			resultFormats = make([]int16, len(ps.FieldDescriptions))
		} else {
			resultFormats = c.resultFormats[:len(ps.FieldDescriptions)]
		}

		for i := range resultFormats {
			if dt, ok := c.ConnInfo.DataTypeForOID(ps.FieldDescriptions[i].DataType); ok {
				if _, ok := dt.Value.(pgtype.BinaryDecoder); ok {
					resultFormats[i] = BinaryFormatCode
				} else {
					resultFormats[i] = TextFormatCode
				}
			}
		}
	}

	rows.resultReader = c.pgConn.ExecPrepared(ctx, ps.Name, paramValues, paramFormats, resultFormats)

	return rows, rows.err
}

// QueryRow is a convenience wrapper over Query. Any error that occurs while
// querying is deferred until calling Scan on the returned Row. That Row will
// error with ErrNoRows if no rows are returned.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	rows, _ := c.Query(ctx, sql, args...)
	return (*connRow)(rows.(*connRows))
}

// SendBatch sends all queued queries to the server at once. All queries are run in an implicit transaction unless
// explicit transaction control statements are executed.
func (c *Conn) SendBatch(ctx context.Context, b *Batch) BatchResults {
	batch := &pgconn.Batch{}

	for _, bi := range b.items {
		var parameterOIDs []pgtype.OID
		ps := c.preparedStatements[bi.query]

		if ps != nil {
			parameterOIDs = ps.ParameterOIDs
		} else {
			parameterOIDs = bi.parameterOIDs
		}

		args, err := convertDriverValuers(bi.arguments)
		if err != nil {
			return &batchResults{err: err}
		}

		paramFormats := make([]int16, len(args))
		paramValues := make([][]byte, len(args))
		for i := range args {
			paramFormats[i] = chooseParameterFormatCode(c.ConnInfo, parameterOIDs[i], args[i])
			paramValues[i], err = newencodePreparedStatementArgument(c.ConnInfo, parameterOIDs[i], args[i])
			if err != nil {
				return &batchResults{err: err}
			}

		}

		if ps != nil {
			resultFormats := bi.resultFormatCodes
			if resultFormats == nil {
				resultFormats = make([]int16, len(ps.FieldDescriptions))
				for i := range resultFormats {
					if dt, ok := c.ConnInfo.DataTypeForOID(ps.FieldDescriptions[i].DataType); ok {
						if _, ok := dt.Value.(pgtype.BinaryDecoder); ok {
							resultFormats[i] = BinaryFormatCode
						} else {
							resultFormats[i] = TextFormatCode
						}
					}
				}
			}

			batch.ExecPrepared(ps.Name, paramValues, paramFormats, resultFormats)
		} else {
			oids := make([]uint32, len(parameterOIDs))
			for i := 0; i < len(parameterOIDs); i++ {
				oids[i] = uint32(parameterOIDs[i])
			}
			batch.ExecParams(bi.query, paramValues, oids, paramFormats, bi.resultFormatCodes)
		}
	}

	mrr := c.pgConn.ExecBatch(ctx, batch)

	return &batchResults{
		conn: c,
		mrr:  mrr,
	}
}
