package pgconn

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgproto3"
)

const batchBufferSize = 4096

var deadlineTime = time.Date(1, 1, 1, 1, 1, 1, 1, time.UTC)

// PgError represents an error reported by the PostgreSQL server. See
// http://www.postgresql.org/docs/11/static/protocol-error-fields.html for
// detailed field description.
type PgError struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         int32
	InternalPosition int32
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	ConstraintName   string
	File             string
	Line             int32
	Routine          string
}

func (pe *PgError) Error() string {
	return pe.Severity + ": " + pe.Message + " (SQLSTATE " + pe.Code + ")"
}

// DialFunc is a function that can be used to connect to a PostgreSQL server
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// ErrTLSRefused occurs when the connection attempt requires TLS and the
// PostgreSQL server refuses to use TLS
var ErrTLSRefused = errors.New("server refused TLS connection")

// PgConn is a low-level PostgreSQL connection handle. It is not safe for concurrent usage.
type PgConn struct {
	conn              net.Conn          // the underlying TCP or unix domain socket connection
	pid               uint32            // backend pid
	secretKey         uint32            // key to use to send a cancel query message to the server
	parameterStatuses map[string]string // parameters that have been reported by the server
	TxStatus          byte
	Frontend          *pgproto3.Frontend

	Config *Config

	batchBuf   []byte
	batchCount int32

	pendingReadyForQueryCount int32

	closed bool

	resultReader PgResultReader
}

// Connect establishes a connection to a PostgreSQL server using the environment and connString (in URL or DSN format)
// to provide configuration. See documention for ParseConfig for details. ctx can be used to cancel a connect attempt.
func Connect(ctx context.Context, connString string) (*PgConn, error) {
	config, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	return ConnectConfig(ctx, config)
}

// Connect establishes a connection to a PostgreSQL server using config. ctx can be used to cancel a connect attempt.
//
// If config.Fallbacks are present they will sequentially be tried in case of error establishing network connection. An
// authentication error will terminate the chain of attempts (like libpq:
// https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-MULTIPLE-HOSTS) and be returned as the error. Otherwise,
// if all attempts fail the last error is returned.
func ConnectConfig(ctx context.Context, config *Config) (pgConn *PgConn, err error) {
	// For convenience set a few defaults if not already set. This makes it simpler to directly construct a config.
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.DialFunc == nil {
		config.DialFunc = makeDefaultDialer().DialContext
	}
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}

	// Simplify usage by treating primary config and fallbacks the same.
	fallbackConfigs := []*FallbackConfig{
		{
			Host:      config.Host,
			Port:      config.Port,
			TLSConfig: config.TLSConfig,
		},
	}
	fallbackConfigs = append(fallbackConfigs, config.Fallbacks...)

	for _, fc := range fallbackConfigs {
		pgConn, err = connect(ctx, config, fc)
		if err == nil {
			return pgConn, nil
		} else if err, ok := err.(*PgError); ok {
			return nil, err
		}
	}

	return nil, err
}

func connect(ctx context.Context, config *Config, fallbackConfig *FallbackConfig) (*PgConn, error) {
	pgConn := new(PgConn)
	pgConn.Config = config

	var err error
	network, address := NetworkAddress(fallbackConfig.Host, fallbackConfig.Port)
	pgConn.conn, err = config.DialFunc(ctx, network, address)
	if err != nil {
		return nil, err
	}

	pgConn.parameterStatuses = make(map[string]string)

	if config.TLSConfig != nil {
		if err := pgConn.startTLS(config.TLSConfig); err != nil {
			pgConn.conn.Close()
			return nil, err
		}
	}

	pgConn.Frontend, err = pgproto3.NewFrontend(pgConn.conn, pgConn.conn)
	if err != nil {
		return nil, err
	}

	startupMsg := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}

	// Copy default run-time params
	for k, v := range config.RuntimeParams {
		startupMsg.Parameters[k] = v
	}

	startupMsg.Parameters["user"] = config.User
	if config.Database != "" {
		startupMsg.Parameters["database"] = config.Database
	}

	if _, err := pgConn.conn.Write(startupMsg.Encode(nil)); err != nil {
		pgConn.conn.Close()
		return nil, err
	}

	for {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *pgproto3.BackendKeyData:
			pgConn.pid = msg.ProcessID
			pgConn.secretKey = msg.SecretKey
		case *pgproto3.Authentication:
			if err = pgConn.rxAuthenticationX(msg); err != nil {
				pgConn.conn.Close()
				return nil, err
			}
		case *pgproto3.ReadyForQuery:
			if config.AfterConnectFunc != nil {
				err := config.AfterConnectFunc(ctx, pgConn)
				if err != nil {
					pgConn.conn.Close()
					return nil, fmt.Errorf("AfterConnectFunc: %v", err)
				}
			}
			return pgConn, nil
		case *pgproto3.ParameterStatus:
			// handled by ReceiveMessage
		case *pgproto3.ErrorResponse:
			pgConn.conn.Close()
			return nil, errorResponseToPgError(msg)
		default:
			pgConn.conn.Close()
			return nil, errors.New("unexpected message")
		}
	}
}

func (pgConn *PgConn) startTLS(tlsConfig *tls.Config) (err error) {
	err = binary.Write(pgConn.conn, binary.BigEndian, []int32{8, 80877103})
	if err != nil {
		return
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(pgConn.conn, response); err != nil {
		return
	}

	if response[0] != 'S' {
		return ErrTLSRefused
	}

	pgConn.conn = tls.Client(pgConn.conn, tlsConfig)

	return nil
}

func (c *PgConn) rxAuthenticationX(msg *pgproto3.Authentication) (err error) {
	switch msg.Type {
	case pgproto3.AuthTypeOk:
	case pgproto3.AuthTypeCleartextPassword:
		err = c.txPasswordMessage(c.Config.Password)
	case pgproto3.AuthTypeMD5Password:
		digestedPassword := "md5" + hexMD5(hexMD5(c.Config.Password+c.Config.User)+string(msg.Salt[:]))
		err = c.txPasswordMessage(digestedPassword)
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func (pgConn *PgConn) txPasswordMessage(password string) (err error) {
	msg := &pgproto3.PasswordMessage{Password: password}
	_, err = pgConn.conn.Write(msg.Encode(nil))
	return err
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}

func (pgConn *PgConn) ReceiveMessage() (pgproto3.BackendMessage, error) {
	msg, err := pgConn.Frontend.Receive()
	if err != nil {
		return nil, err
	}

	switch msg := msg.(type) {
	case *pgproto3.ReadyForQuery:
		// Under normal circumstances pendingReadyForQueryCount will be > 0 when a
		// ReadyForQuery is received. However, this is not the case on initial
		// connection.
		if pgConn.pendingReadyForQueryCount > 0 {
			pgConn.pendingReadyForQueryCount -= 1
		}
		pgConn.TxStatus = msg.TxStatus
	case *pgproto3.ParameterStatus:
		pgConn.parameterStatuses[msg.Name] = msg.Value
	case *pgproto3.ErrorResponse:
		if msg.Severity == "FATAL" {
			// TODO - close pgConn
			return nil, errorResponseToPgError(msg)
		}
	}

	return msg, nil
}

// Conn returns the underlying net.Conn.
func (pgConn *PgConn) Conn() net.Conn {
	return pgConn.conn
}

// PID returns the backend PID.
func (pgConn *PgConn) PID() uint32 {
	return pgConn.pid
}

// SecretKey returns the backend secret key used to send a cancel query message to the server.
func (pgConn *PgConn) SecretKey() uint32 {
	return pgConn.secretKey
}

// Close closes a connection. It is safe to call Close on a already closed connection. Close attempts a clean close by
// sending the exit message to PostgreSQL. However, this could block so ctx is available to limit the time to wait. The
// underlying net.Conn.Close() will always be called regardless of any other errors.
func (pgConn *PgConn) Close(ctx context.Context) error {
	if pgConn.closed {
		return nil
	}
	pgConn.closed = true

	defer pgConn.conn.Close()

	cleanupContext := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContext()

	_, err := pgConn.conn.Write([]byte{'X', 0, 0, 0, 4})
	if err != nil {
		return preferContextOverNetTimeoutError(ctx, err)
	}

	_, err = pgConn.conn.Read(make([]byte, 1))
	if err != io.EOF {
		return preferContextOverNetTimeoutError(ctx, err)
	}

	return pgConn.conn.Close()
}

// ParameterStatus returns the value of a parameter reported by the server (e.g.
// server_version). Returns an empty string for unknown parameters.
func (pgConn *PgConn) ParameterStatus(key string) string {
	return pgConn.parameterStatuses[key]
}

// CommandTag is the result of an Exec function
type CommandTag []byte

// RowsAffected returns the number of rows affected. If the CommandTag was not
// for a row affecting command (e.g. "CREATE TABLE") then it returns 0.
func (ct CommandTag) RowsAffected() int64 {
	s := string(ct)
	index := strings.LastIndex(s, " ")
	if index == -1 {
		return 0
	}
	n, _ := strconv.ParseInt(s[index+1:], 10, 64)
	return n
}

func (ct CommandTag) String() string {
	return string(ct)
}

// SendExec enqueues the execution of sql via the PostgreSQL simple query protocol. sql may contain multiple queries.
// Execution is implicitly wrapped in a transactions unless a transaction is already in progress or sql contains
// transaction control statements. It is only sent to the PostgreSQL server when Flush is called.
func (pgConn *PgConn) SendExec(sql string) {
	pgConn.batchBuf = appendQuery(pgConn.batchBuf, sql)
	pgConn.batchCount += 1
}

// appendQuery appends a PostgreSQL wire protocol query message to buf and returns it.
func appendQuery(buf []byte, query string) []byte {
	buf = append(buf, 'Q')
	buf = pgio.AppendInt32(buf, int32(len(query)+5))
	buf = append(buf, query...)
	buf = append(buf, 0)
	return buf
}

// appendParse appends a PostgreSQL wire protocol parse message to buf and returns it.
func appendParse(buf []byte, name string, query string, paramOIDs []uint32) []byte {
	if len(paramOIDs) > 65535 {
		panic(fmt.Sprintf("len(paramOIDs) must be between 0 and 65535, received %d", len(paramOIDs)))
	}

	buf = append(buf, 'P')
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	buf = append(buf, name...)
	buf = append(buf, 0)
	buf = append(buf, query...)
	buf = append(buf, 0)

	buf = pgio.AppendInt16(buf, int16(len(paramOIDs)))
	for _, oid := range paramOIDs {
		buf = pgio.AppendUint32(buf, oid)
	}
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	return buf
}

// appendDescribe appends a PostgreSQL wire protocol describe message to buf and returns it.
func appendDescribe(buf []byte, objectType byte, name string) []byte {
	buf = append(buf, 'D')
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	buf = append(buf, objectType)
	buf = append(buf, name...)
	buf = append(buf, 0)
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	return buf
}

// appendSync appends a PostgreSQL wire protocol sync message to buf and returns it.
func appendSync(buf []byte) []byte {
	buf = append(buf, 'S')
	buf = pgio.AppendInt32(buf, 4)

	return buf
}

// appendBind appends a PostgreSQL wire protocol bind message to buf and returns it.
func appendBind(
	buf []byte,
	destinationPortal,
	preparedStatement string,
	paramFormats []int16,
	paramValues [][]byte,
	resultFormatCodes []int16,
) []byte {
	if len(paramFormats) != 0 && len(paramFormats) != len(paramValues) && len(paramFormats) != len(paramValues) {
		panic(fmt.Sprintf("len(paramFormats) must be 0, 1, or len(paramValues), received %d", len(paramFormats)))
	}
	if len(paramValues) > 65535 {
		panic(fmt.Sprintf("len(paramValues) must be between 0 and 65535, received %d", len(paramValues)))
	}

	buf = append(buf, 'B')
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	buf = append(buf, destinationPortal...)
	buf = append(buf, 0)
	buf = append(buf, preparedStatement...)
	buf = append(buf, 0)

	buf = pgio.AppendInt16(buf, int16(len(paramFormats)))
	for _, f := range paramFormats {
		buf = pgio.AppendInt16(buf, f)
	}

	buf = pgio.AppendInt16(buf, int16(len(paramValues)))
	for _, p := range paramValues {
		if p == nil {
			buf = pgio.AppendInt32(buf, -1)
			continue
		}

		buf = pgio.AppendInt32(buf, int32(len(p)))
		buf = append(buf, p...)
	}

	buf = pgio.AppendInt16(buf, int16(len(resultFormatCodes)))
	for _, fc := range resultFormatCodes {
		buf = pgio.AppendInt16(buf, fc)
	}
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	return buf
}

// appendExecute appends a PostgreSQL wire protocol execute message to buf and returns it.
func appendExecute(buf []byte, portal string, maxRows uint32) []byte {
	buf = append(buf, 'E')
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)

	buf = append(buf, portal...)
	buf = append(buf, 0)
	buf = pgio.AppendUint32(buf, maxRows)

	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	return buf
}

// SendExecParams enqueues the execution of sql via the PostgreSQL extended query protocol.
//
// sql is a SQL command string. It may only contain one query. Parameter substitution is position using $1, $2, $3, etc.
//
// paramValues are the parameter values. It must be encoded in the format given by paramFormats.
//
// paramOIDs is a slice of data type OIDs for paramValues. If paramOIDs is nil, the server will infer the data type for
// all parameters. Any paramOID element that is 0 that will cause the server to infer the data type for that parameter.
// SendExecParams will panic if len(paramOIDs) is not 0, 1, or len(paramValues).
//
// paramFormats is a slice of format codes determining for each paramValue column whether it is encoded in text or
// binary format. If paramFormats is nil all results will be in text protocol. SendExecParams will panic if
// len(paramFormats) is not 0, 1, or len(paramValues).
//
// resultFormats is a slice of format codes determining for each result column whether it is encoded in text or
// binary format. If resultFormats is nil all results will be in text protocol.
//
// Query is only sent to the PostgreSQL server when Flush is called.
func (pgConn *PgConn) SendExecParams(sql string, paramValues [][]byte, paramOIDs []uint32, paramFormats []int16, resultFormats []int16) {
	if len(paramOIDs) != 0 && len(paramOIDs) != len(paramValues) && len(paramOIDs) != len(paramValues) {
		panic(fmt.Sprintf("len(paramOIDs) must be 0, 1, or len(paramValues), received %d", len(paramOIDs)))
	}

	pgConn.batchBuf = appendParse(pgConn.batchBuf, "", sql, paramOIDs)
	pgConn.batchBuf = appendDescribe(pgConn.batchBuf, 'S', "")
	pgConn.batchBuf = appendBind(pgConn.batchBuf, "", "", paramFormats, paramValues, resultFormats)
	pgConn.batchBuf = appendExecute(pgConn.batchBuf, "", 0)
	pgConn.batchBuf = appendSync(pgConn.batchBuf)
	pgConn.batchCount += 1
}

// SendExecPrepared enqueues the execution of a prepared statement via the PostgreSQL extended query protocol.
//
// paramValues are the parameter values. It must be encoded in the format given by paramFormats.
//
// paramFormats is a slice of format codes determining for each paramValue column whether it is encoded in text or
// binary format. If paramFormats is nil all results will be in text protocol. SendExecParams will panic if
// len(paramFormats) is not 0, 1, or len(paramValues).
//
// resultFormats is a slice of format codes determining for each result column whether it is encoded in text or
// binary format. If resultFormats is nil all results will be in text protocol.
//
// Query is only sent to the PostgreSQL server when Flush is called.
func (pgConn *PgConn) SendExecPrepared(stmtName string, paramValues [][]byte, paramFormats []int16, resultFormats []int16) {
	pgConn.batchBuf = appendDescribe(pgConn.batchBuf, 'S', stmtName)
	pgConn.batchBuf = appendBind(pgConn.batchBuf, "", stmtName, paramFormats, paramValues, resultFormats)
	pgConn.batchBuf = appendExecute(pgConn.batchBuf, "", 0)
	pgConn.batchBuf = appendSync(pgConn.batchBuf)
	pgConn.batchCount += 1
}

type PgResultReader struct {
	pgConn             *PgConn
	fieldDescriptions  []pgproto3.FieldDescription
	rowValues          [][]byte
	commandTag         CommandTag
	err                error
	complete           bool
	preloadedRowValues bool
	ctx                context.Context
	cleanupContext     func()
}

// NextResult reads until a result is ready to be read or no results are pending. Returns true if a result is available.
// Use ResultReader() to acquire a reader for the result.
func (pgConn *PgConn) NextResult(ctx context.Context) bool {
	cleanupContext := contextDoneToConnDeadline(ctx, pgConn.conn)

	for pgConn.pendingReadyForQueryCount > 0 {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			cleanupContext()
			pgConn.resultReader = PgResultReader{pgConn: pgConn, ctx: ctx, err: preferContextOverNetTimeoutError(ctx, err), complete: true}
			return true
		}

		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			pgConn.resultReader = PgResultReader{pgConn: pgConn, ctx: ctx, cleanupContext: cleanupContext, fieldDescriptions: msg.Fields}
			return true
		case *pgproto3.DataRow:
			pgConn.resultReader = PgResultReader{pgConn: pgConn, ctx: ctx, cleanupContext: cleanupContext, rowValues: msg.Values, preloadedRowValues: true}
			return true
		case *pgproto3.CommandComplete:
			cleanupContext()
			pgConn.resultReader = PgResultReader{pgConn: pgConn, ctx: ctx, commandTag: CommandTag(msg.CommandTag), complete: true}
			return true
		case *pgproto3.ErrorResponse:
			cleanupContext()
			pgConn.resultReader = PgResultReader{pgConn: pgConn, ctx: ctx, err: errorResponseToPgError(msg), complete: true}
			return true
		}
	}

	cleanupContext()
	return false
}

// ResultReader returns the result reader prepared by next result. It is only valid until the result is completed.
func (pgConn *PgConn) ResultReader() *PgResultReader {
	return &pgConn.resultReader
}

// NextRow returns advances the PgResultReader to the next row and returns true if a row is available.
func (rr *PgResultReader) NextRow() bool {
	if rr.complete {
		return false
	}

	if rr.preloadedRowValues {
		rr.preloadedRowValues = false
		return true
	}

	for {
		msg, err := rr.pgConn.ReceiveMessage()
		if err != nil {
			rr.err = preferContextOverNetTimeoutError(rr.ctx, err)
			rr.close()
			return false
		}

		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			rr.fieldDescriptions = msg.Fields
		case *pgproto3.DataRow:
			rr.rowValues = msg.Values
			return true
		case *pgproto3.CommandComplete:
			rr.commandTag = CommandTag(msg.CommandTag)
			rr.close()
			return false
		case *pgproto3.ErrorResponse:
			rr.err = errorResponseToPgError(msg)
			rr.close()
			return false
		}
	}
}

// FieldDescriptions returns the field descriptions for the current result set. The returned slice is only valid until
// the PgResultReader is closed.
func (rr *PgResultReader) FieldDescriptions() []pgproto3.FieldDescription {
	return rr.fieldDescriptions
}

// Values returns the current row data. NextRow must have been previously been called. The returned [][]byte is only
// valid until the next NextRow call or the PgResultReader is closed. However, the underlying byte data is safe to
// retain a reference to and mutate.
func (rr *PgResultReader) Values() [][]byte {
	return rr.rowValues
}

// Close consumes any remaining result data and returns the command tag or
// error.
func (rr *PgResultReader) Close() (CommandTag, error) {
	if rr.complete {
		return rr.commandTag, rr.err
	}
	defer rr.close()

	for {
		msg, err := rr.pgConn.ReceiveMessage()
		if err != nil {
			rr.err = preferContextOverNetTimeoutError(rr.ctx, err)
			return rr.commandTag, rr.err
		}

		switch msg := msg.(type) {
		case *pgproto3.CommandComplete:
			rr.commandTag = CommandTag(msg.CommandTag)
			return rr.commandTag, rr.err
		case *pgproto3.ErrorResponse:
			rr.err = errorResponseToPgError(msg)
			return rr.commandTag, rr.err
		}
	}
}

func (rr *PgResultReader) close() {
	if rr.complete {
		return
	}

	rr.cleanupContext()
	rr.rowValues = nil
	rr.complete = true
}

// Flush sends the enqueued execs to the server.
func (pgConn *PgConn) Flush(ctx context.Context) error {
	defer pgConn.resetBatch()

	cleanup := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanup()

	n, err := pgConn.conn.Write(pgConn.batchBuf)
	if err != nil {
		if n > 0 {
			// Close connection because cannot recover from partially sent message.
			pgConn.conn.Close()
			pgConn.closed = true
		}
		return preferContextOverNetTimeoutError(ctx, err)
	}

	pgConn.pendingReadyForQueryCount += pgConn.batchCount
	return nil
}

// contextDoneToConnDeadline starts a goroutine that will set an immediate deadline on conn after reading from
// ctx.Done(). The returned cleanup function must be called to terminate this goroutine. The cleanup function is safe to
// call multiple times.
func contextDoneToConnDeadline(ctx context.Context, conn net.Conn) (cleanup func()) {
	if ctx.Done() != nil {
		deadlineWasSet := false
		doneChan := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				conn.SetDeadline(deadlineTime)
				deadlineWasSet = true
				<-doneChan
				// TODO
			case <-doneChan:
			}
		}()

		finished := false
		return func() {
			if !finished {
				doneChan <- struct{}{}
				if deadlineWasSet {
					conn.SetDeadline(time.Time{})
				}
				finished = true
			}
		}
	}

	return func() {}
}

// preferContextOverNetTimeoutError returns ctx.Err() if ctx.Err() is present and err is a net.Error with Timeout() ==
// true. Otherwise returns err.
func preferContextOverNetTimeoutError(ctx context.Context, err error) error {
	if err, ok := err.(net.Error); ok && err.Timeout() && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// RecoverFromTimeout attempts to recover from a timeout error such as is caused by a canceled context. If recovery is
// successful true is returned. If recovery is not successful the connection is closed and false it returned. Recovery
// should usually be possible except in the case of a partial write. This must be called after any context cancellation.
//
// As RecoverFromTimeout may need to read and ignored data already sent from the server, it potentially can block
// indefinitely. Use ctx to guard against this.
func (pgConn *PgConn) RecoverFromTimeout(ctx context.Context) bool {
	if pgConn.closed {
		return false
	}
	pgConn.resetBatch()

	// Clear any existing timeout
	pgConn.conn.SetDeadline(time.Time{})

	// Try to cancel any in-progress requests
	for i := 0; i < int(pgConn.pendingReadyForQueryCount); i++ {
		pgConn.CancelRequest(ctx)
	}

	cleanupContext := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContext()

	for pgConn.pendingReadyForQueryCount > 0 {
		_, err := pgConn.ReceiveMessage()
		if err != nil {
			preferContextOverNetTimeoutError(ctx, err)
			pgConn.Close(context.Background())
			return false
		}
	}

	result, err := pgConn.Exec(
		context.Background(), // do not use ctx again because deadline goroutine already started above
		"select 'RecoverFromTimeout'",
	)
	if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || string(result.Rows[0][0]) != "RecoverFromTimeout" {
		pgConn.Close(context.Background())
		return false
	}

	return true
}

func (pgConn *PgConn) resetBatch() {
	pgConn.batchCount = 0
	if len(pgConn.batchBuf) > batchBufferSize {
		pgConn.batchBuf = make([]byte, 0, batchBufferSize)
	} else {
		pgConn.batchBuf = pgConn.batchBuf[0:0]
	}
}

type PgResult struct {
	Rows       [][][]byte
	CommandTag CommandTag
}

// Exec executes sql via the PostgreSQL simple query protocol, buffers the entire result, and returns it. sql may
// contain multiple queries, but only the last results will be returned. Execution is implicitly wrapped in a
// transactions unless a transaction is already in progress or sql contains transaction control statements.
//
// Exec must not be called when there are pending results from previous Send* methods (e.g. SendExec).
func (pgConn *PgConn) Exec(ctx context.Context, sql string) (*PgResult, error) {
	if pgConn.batchCount != 0 {
		return nil, errors.New("unflushed previous sends")
	}
	if pgConn.pendingReadyForQueryCount != 0 {
		return nil, errors.New("unread previous results")
	}

	pgConn.SendExec(sql)
	err := pgConn.Flush(ctx)
	if err != nil {
		return nil, err
	}

	return pgConn.bufferLastResult(ctx)
}

func (pgConn *PgConn) bufferLastResult(ctx context.Context) (*PgResult, error) {
	var result *PgResult

	for pgConn.NextResult(ctx) {
		resultReader := pgConn.ResultReader()
		rows := [][][]byte{}
		for resultReader.NextRow() {
			row := make([][]byte, len(resultReader.Values()))
			copy(row, resultReader.Values())
			rows = append(rows, row)
		}

		commandTag, err := resultReader.Close()
		if err != nil {
			return nil, err
		}

		result = &PgResult{
			Rows:       rows,
			CommandTag: commandTag,
		}
	}
	if result == nil {
		return nil, errors.New("unexpected missing result")
	}

	return result, nil
}

// ExecParams executes sql via the PostgreSQL extended query protocol, buffers the entire result, and returns it. See
// SendExecParams for parameter descriptions.
//
// ExecParams must not be called when there are pending results from previous Send* methods (e.g. SendExec).
func (pgConn *PgConn) ExecParams(ctx context.Context, sql string, paramValues [][]byte, paramOIDs []uint32, paramFormats []int16, resultFormats []int16) (*PgResult, error) {
	if pgConn.batchCount != 0 {
		return nil, errors.New("unflushed previous sends")
	}
	if pgConn.pendingReadyForQueryCount != 0 {
		return nil, errors.New("unread previous results")
	}

	pgConn.SendExecParams(sql, paramValues, paramOIDs, paramFormats, resultFormats)
	err := pgConn.Flush(ctx)
	if err != nil {
		return nil, err
	}

	return pgConn.bufferLastResult(ctx)
}

// ExecPrepared executes a prepared statement via the PostgreSQL extended query protocol, buffers the entire result, and
// returns it. See SendExecPrepared for parameter descriptions.
//
// ExecPrepared must not be called when there are pending results from previous Send* methods (e.g. SendExec).
func (pgConn *PgConn) ExecPrepared(ctx context.Context, stmtName string, paramValues [][]byte, paramFormats []int16, resultFormats []int16) (*PgResult, error) {
	if pgConn.batchCount != 0 {
		return nil, errors.New("unflushed previous sends")
	}
	if pgConn.pendingReadyForQueryCount != 0 {
		return nil, errors.New("unread previous results")
	}

	pgConn.SendExecPrepared(stmtName, paramValues, paramFormats, resultFormats)
	err := pgConn.Flush(ctx)
	if err != nil {
		return nil, err
	}

	return pgConn.bufferLastResult(ctx)
}

// Prepare creates a prepared statement.
func (pgConn *PgConn) Prepare(ctx context.Context, name, sql string, paramOIDs []uint32) error {
	if pgConn.batchCount != 0 {
		return errors.New("unflushed previous sends")
	}
	if pgConn.pendingReadyForQueryCount != 0 {
		return errors.New("unread previous results")
	}

	cleanupContext := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContext()

	pgConn.batchBuf = appendParse(pgConn.batchBuf, name, sql, paramOIDs)
	pgConn.batchBuf = appendDescribe(pgConn.batchBuf, 'S', name)
	pgConn.batchBuf = appendSync(pgConn.batchBuf)
	pgConn.batchCount += 1
	err := pgConn.Flush(context.Background())
	if err != nil {
		return preferContextOverNetTimeoutError(ctx, err)
	}

	for pgConn.pendingReadyForQueryCount > 0 {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			return preferContextOverNetTimeoutError(ctx, err)
		}

		switch msg := msg.(type) {
		case *pgproto3.ParameterDescription:
			// TODO
		case *pgproto3.RowDescription:
			// TODO
		case *pgproto3.ErrorResponse:
			return errorResponseToPgError(msg)
		}
	}

	return nil
}

func errorResponseToPgError(msg *pgproto3.ErrorResponse) *PgError {
	return &PgError{
		Severity:         string(msg.Severity),
		Code:             string(msg.Code),
		Message:          string(msg.Message),
		Detail:           string(msg.Detail),
		Hint:             string(msg.Hint),
		Position:         msg.Position,
		InternalPosition: msg.InternalPosition,
		InternalQuery:    string(msg.InternalQuery),
		Where:            string(msg.Where),
		SchemaName:       string(msg.SchemaName),
		TableName:        string(msg.TableName),
		ColumnName:       string(msg.ColumnName),
		DataTypeName:     string(msg.DataTypeName),
		ConstraintName:   string(msg.ConstraintName),
		File:             string(msg.File),
		Line:             msg.Line,
		Routine:          string(msg.Routine),
	}
}

// CancelRequest sends a cancel request to the PostgreSQL server. It returns an error if unable to deliver the cancel
// request, but lack of an error does not ensure that the query was canceled. As specified in the documentation, there
// is no way to be sure a query was canceled. See https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.9
func (pgConn *PgConn) CancelRequest(ctx context.Context) error {
	// Open a cancellation request to the same server. The address is taken from the net.Conn directly instead of reusing
	// the connection config. This is important in high availability configurations where fallback connections may be
	// specified or DNS may be used to load balance.
	serverAddr := pgConn.conn.RemoteAddr()
	cancelConn, err := pgConn.Config.DialFunc(ctx, serverAddr.Network(), serverAddr.String())
	if err != nil {
		return err
	}
	defer cancelConn.Close()

	cleanupContext := contextDoneToConnDeadline(ctx, cancelConn)
	defer cleanupContext()

	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], 16)
	binary.BigEndian.PutUint32(buf[4:8], 80877102)
	binary.BigEndian.PutUint32(buf[8:12], uint32(pgConn.pid))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pgConn.secretKey))
	_, err = cancelConn.Write(buf)
	if err != nil {
		return preferContextOverNetTimeoutError(ctx, err)
	}

	_, err = cancelConn.Read(buf)
	if err != io.EOF {
		return fmt.Errorf("Server failed to close connection after cancel query request: %v", preferContextOverNetTimeoutError(ctx, err))
	}

	return nil
}
