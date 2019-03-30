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
	"sync"
	"time"

	"github.com/jackc/pgio"
	"github.com/jackc/pgproto3"
)

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

// Notice represents a notice response message reported by the PostgreSQL server. Be aware that this is distinct from
// LISTEN/NOTIFY notification.
type Notice PgError

// Notification is a message received from the PostgreSQL LISTEN/NOTIFY system
type Notification struct {
	PID     uint32 // backend pid that sent the notification
	Channel string // channel from which notification was received
	Payload string
}

// DialFunc is a function that can be used to connect to a PostgreSQL server
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// NoticeHandler is a function that can handle notices received from the PostgreSQL server. Notices can be received at
// any time, usually during handling of a query response. The *PgConn is provided so the handler is aware of the origin
// of the notice, but it must not invoke any query method. Be aware that this is distinct from LISTEN/NOTIFY
// notification.
type NoticeHandler func(*PgConn, *Notice)

// NotificationHandler is a function that can handle notifications received from the PostgreSQL server. Notifications
// can be received at any time, usually during handling of a query response. The *PgConn is provided so the handler is
// aware of the origin of the notice, but it must not invoke any query method. Be aware that this is distinct from a
// notice event.
type NotificationHandler func(*PgConn, *Notification)

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

	controller chan interface{}

	closed bool

	bufferingReceive    bool
	bufferingReceiveMux sync.Mutex
	bufferingReceiveMsg pgproto3.BackendMessage
	bufferingReceiveErr error
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
	pgConn.controller = make(chan interface{}, 1)

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

	pgConn.Frontend, err = pgproto3.NewFrontend(pgproto3.NewChunkReader(pgConn.conn), pgConn.conn)
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
			pgConn.conn.Close()
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

func (pgConn *PgConn) signalMessage() chan struct{} {
	if pgConn.bufferingReceive {
		panic("BUG: signalMessage when already in progress")
	}

	pgConn.bufferingReceive = true
	pgConn.bufferingReceiveMux.Lock()

	ch := make(chan struct{})
	go func() {
		pgConn.bufferingReceiveMsg, pgConn.bufferingReceiveErr = pgConn.Frontend.Receive()
		pgConn.bufferingReceiveMux.Unlock()
		close(ch)
	}()

	return ch
}

func (pgConn *PgConn) ReceiveMessage() (pgproto3.BackendMessage, error) {
	var msg pgproto3.BackendMessage
	var err error
	if pgConn.bufferingReceive {
		pgConn.bufferingReceiveMux.Lock()
		msg = pgConn.bufferingReceiveMsg
		err = pgConn.bufferingReceiveErr
		pgConn.bufferingReceiveMux.Unlock()
		pgConn.bufferingReceive = false

		// If a timeout error happened in the background try the read again.
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			msg, err = pgConn.Frontend.Receive()
		}
	} else {
		msg, err = pgConn.Frontend.Receive()
	}

	if err != nil {
		// Close on anything other than timeout error - everything else is fatal
		if err, ok := err.(net.Error); !(ok && err.Timeout()) {
			pgConn.hardClose()
		}

		return nil, err
	}

	switch msg := msg.(type) {
	case *pgproto3.ReadyForQuery:
		pgConn.TxStatus = msg.TxStatus
	case *pgproto3.ParameterStatus:
		pgConn.parameterStatuses[msg.Name] = msg.Value
	case *pgproto3.ErrorResponse:
		if msg.Severity == "FATAL" {
			pgConn.hardClose()
			return nil, errorResponseToPgError(msg)
		}
	case *pgproto3.NoticeResponse:
		if pgConn.Config.OnNotice != nil {
			pgConn.Config.OnNotice(pgConn, noticeResponseToNotice(msg))
		}
	case *pgproto3.NotificationResponse:
		if pgConn.Config.OnNotification != nil {
			pgConn.Config.OnNotification(pgConn, &Notification{PID: msg.PID, Channel: msg.Channel, Payload: msg.Payload})
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

// hardClose closes the underlying connection without sending the exit message.
func (pgConn *PgConn) hardClose() error {
	if pgConn.closed {
		return nil
	}
	pgConn.closed = true
	return pgConn.conn.Close()
}

// TODO - rethink how to report status. At the moment this is just a temporary measure so pgx.Conn can detect deatch of
// underlying connection.
func (pgConn *PgConn) IsAlive() bool {
	return !pgConn.closed
}

// ParameterStatus returns the value of a parameter reported by the server (e.g.
// server_version). Returns an empty string for unknown parameters.
func (pgConn *PgConn) ParameterStatus(key string) string {
	return pgConn.parameterStatuses[key]
}

// CommandTag is the result of an Exec function
type CommandTag string

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

// preferContextOverNetTimeoutError returns ctx.Err() if ctx.Err() is present and err is a net.Error with Timeout() ==
// true. Otherwise returns err.
func preferContextOverNetTimeoutError(ctx context.Context, err error) error {
	if err, ok := err.(net.Error); ok && err.Timeout() && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
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

type PreparedStatementDescription struct {
	Name      string
	SQL       string
	ParamOIDs []uint32
	Fields    []pgproto3.FieldDescription
}

// Prepare creates a prepared statement.
func (pgConn *PgConn) Prepare(ctx context.Context, name, sql string, paramOIDs []uint32) (*PreparedStatementDescription, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pgConn.controller <- pgConn:
	}
	cleanupContextDeadline := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContextDeadline()

	var buf []byte
	buf = (&pgproto3.Parse{Name: name, Query: sql, ParameterOIDs: paramOIDs}).Encode(buf)
	buf = (&pgproto3.Describe{ObjectType: 'S', Name: name}).Encode(buf)
	buf = (&pgproto3.Sync{}).Encode(buf)

	_, err := pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		return nil, preferContextOverNetTimeoutError(ctx, err)
	}

	psd := &PreparedStatementDescription{Name: name, SQL: sql}

	var parseErr error

readloop:
	for {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			pgConn.hardClose()
			return nil, preferContextOverNetTimeoutError(ctx, err)
		}

		switch msg := msg.(type) {
		case *pgproto3.ParameterDescription:
			psd.ParamOIDs = make([]uint32, len(msg.ParameterOIDs))
			copy(psd.ParamOIDs, msg.ParameterOIDs)
		case *pgproto3.RowDescription:
			psd.Fields = make([]pgproto3.FieldDescription, len(msg.Fields))
			copy(psd.Fields, msg.Fields)
		case *pgproto3.ErrorResponse:
			parseErr = errorResponseToPgError(msg)
		case *pgproto3.ReadyForQuery:
			break readloop
		}
	}

	<-pgConn.controller

	if parseErr != nil {
		return nil, parseErr
	}
	return psd, nil
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

func noticeResponseToNotice(msg *pgproto3.NoticeResponse) *Notice {
	pgerr := errorResponseToPgError((*pgproto3.ErrorResponse)(msg))
	return (*Notice)(pgerr)
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

// WaitForNotification waits for a LISTON/NOTIFY message to be received. It returns an error if a notification was not
// received.
func (pgConn *PgConn) WaitForNotification(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pgConn.controller <- pgConn:
	}
	cleanupContextDeadline := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContextDeadline()
	defer func() { <-pgConn.controller }()

	for {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			return preferContextOverNetTimeoutError(ctx, err)
		}

		switch msg.(type) {
		case *pgproto3.NotificationResponse:
			return nil
		}
	}
}

// Exec executes SQL via the PostgreSQL simple query protocol. SQL may contain multiple queries. Execution is
// implicitly wrapped in a transaction unless a transaction is already in progress or SQL contains transaction control
// statements.
//
// Prefer ExecParams unless executing arbitrary SQL that may contain multiple queries.
func (pgConn *PgConn) Exec(ctx context.Context, sql string) *MultiResultReader {
	multiResult := &MultiResultReader{
		pgConn:                 pgConn,
		ctx:                    ctx,
		cleanupContextDeadline: func() {},
	}

	select {
	case <-ctx.Done():
		multiResult.closed = true
		multiResult.err = ctx.Err()
		return multiResult
	case pgConn.controller <- multiResult:
	}
	multiResult.cleanupContextDeadline = contextDoneToConnDeadline(ctx, pgConn.conn)

	var buf []byte
	buf = (&pgproto3.Query{String: sql}).Encode(buf)

	_, err := pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		multiResult.cleanupContextDeadline()
		multiResult.closed = true
		multiResult.err = preferContextOverNetTimeoutError(ctx, err)
		<-pgConn.controller
		return multiResult
	}

	return multiResult
}

// ExecParams executes a command via the PostgreSQL extended query protocol.
//
// sql is a SQL command string. It may only contain one query. Parameter substitution is positional using $1, $2, $3,
// etc.
//
// paramValues are the parameter values. It must be encoded in the format given by paramFormats.
//
// paramOIDs is a slice of data type OIDs for paramValues. If paramOIDs is nil, the server will infer the data type for
// all parameters. Any paramOID element that is 0 that will cause the server to infer the data type for that parameter.
// ExecParams will panic if len(paramOIDs) is not 0, 1, or len(paramValues).
//
// paramFormats is a slice of format codes determining for each paramValue column whether it is encoded in text or
// binary format. If paramFormats is nil all results will be in text protocol. ExecParams will panic if
// len(paramFormats) is not 0, 1, or len(paramValues).
//
// resultFormats is a slice of format codes determining for each result column whether it is encoded in text or
// binary format. If resultFormats is nil all results will be in text protocol.
//
// ResultReader must be closed before PgConn can be used again.
func (pgConn *PgConn) ExecParams(ctx context.Context, sql string, paramValues [][]byte, paramOIDs []uint32, paramFormats []int16, resultFormats []int16) *ResultReader {
	result := &ResultReader{
		pgConn:                 pgConn,
		ctx:                    ctx,
		cleanupContextDeadline: func() {},
	}

	select {
	case <-ctx.Done():
		result.concludeCommand("", ctx.Err())
		result.closed = true
		return result
	case pgConn.controller <- result:
	}
	result.cleanupContextDeadline = contextDoneToConnDeadline(ctx, pgConn.conn)

	var buf []byte

	// TODO - refactor ExecParams and ExecPrepared - these lines only difference
	buf = (&pgproto3.Parse{Query: sql, ParameterOIDs: paramOIDs}).Encode(buf)
	buf = (&pgproto3.Bind{ParameterFormatCodes: paramFormats, Parameters: paramValues, ResultFormatCodes: resultFormats}).Encode(buf)

	buf = (&pgproto3.Describe{ObjectType: 'P'}).Encode(buf)
	buf = (&pgproto3.Execute{}).Encode(buf)
	buf = (&pgproto3.Sync{}).Encode(buf)

	_, err := pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		result.concludeCommand("", err)
		result.cleanupContextDeadline()
		result.closed = true
		<-pgConn.controller
	}

	return result
}

// ExecPrepared enqueues the execution of a prepared statement via the PostgreSQL extended query protocol.
//
// paramValues are the parameter values. It must be encoded in the format given by paramFormats.
//
// paramFormats is a slice of format codes determining for each paramValue column whether it is encoded in text or
// binary format. If paramFormats is nil all results will be in text protocol. ExecPrepared will panic if
// len(paramFormats) is not 0, 1, or len(paramValues).
//
// resultFormats is a slice of format codes determining for each result column whether it is encoded in text or
// binary format. If resultFormats is nil all results will be in text protocol.
//
// ResultReader must be closed before PgConn can be used again.
func (pgConn *PgConn) ExecPrepared(ctx context.Context, stmtName string, paramValues [][]byte, paramFormats []int16, resultFormats []int16) *ResultReader {
	result := &ResultReader{
		pgConn:                 pgConn,
		ctx:                    ctx,
		cleanupContextDeadline: func() {},
	}

	select {
	case <-ctx.Done():
		result.concludeCommand("", ctx.Err())
		result.closed = true
		return result
	case pgConn.controller <- result:
	}
	result.cleanupContextDeadline = contextDoneToConnDeadline(ctx, pgConn.conn)

	var buf []byte
	buf = (&pgproto3.Bind{PreparedStatement: stmtName, ParameterFormatCodes: paramFormats, Parameters: paramValues, ResultFormatCodes: resultFormats}).Encode(buf)
	buf = (&pgproto3.Describe{ObjectType: 'P'}).Encode(buf)
	buf = (&pgproto3.Execute{}).Encode(buf)
	buf = (&pgproto3.Sync{}).Encode(buf)

	_, err := pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		result.concludeCommand("", err)
		result.cleanupContextDeadline()
		result.closed = true
		<-pgConn.controller
	}

	return result
}

// CopyTo executes the copy command sql and copies the results to w.
func (pgConn *PgConn) CopyTo(ctx context.Context, w io.Writer, sql string) (CommandTag, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case pgConn.controller <- pgConn:
	}
	cleanupContextDeadline := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContextDeadline()

	// Send copy to command
	var buf []byte
	buf = (&pgproto3.Query{String: sql}).Encode(buf)

	_, err := pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		<-pgConn.controller

		return "", preferContextOverNetTimeoutError(ctx, err)
	}

	// Read results
	var commandTag CommandTag
	var pgErr error
	for {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			pgConn.hardClose()
			return "", preferContextOverNetTimeoutError(ctx, err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyDone:
		case *pgproto3.CopyData:
			_, err := w.Write(msg.Data)
			if err != nil {
				pgConn.hardClose()
				return "", err
			}
		case *pgproto3.ReadyForQuery:
			<-pgConn.controller
			return commandTag, pgErr
		case *pgproto3.CommandComplete:
			commandTag = CommandTag(msg.CommandTag)
		case *pgproto3.ErrorResponse:
			pgErr = errorResponseToPgError(msg)
		}
	}
}

// CopyFrom executes the copy command sql and copies all of r to the PostgreSQL server.
//
// Note: context cancellation will only interrupt operations on the underlying PostgreSQL network connection. Reads on r
// could still block.
func (pgConn *PgConn) CopyFrom(ctx context.Context, r io.Reader, sql string) (CommandTag, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case pgConn.controller <- pgConn:
	}
	cleanupContextDeadline := contextDoneToConnDeadline(ctx, pgConn.conn)
	defer cleanupContextDeadline()

	// Send copy to command
	var buf []byte
	buf = (&pgproto3.Query{String: sql}).Encode(buf)

	_, err := pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		<-pgConn.controller

		return "", preferContextOverNetTimeoutError(ctx, err)
	}

	// Read until copy in response or error.
	var commandTag CommandTag
	var pgErr error
	pendingCopyInResponse := true
	for pendingCopyInResponse {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			pgConn.hardClose()
			return "", preferContextOverNetTimeoutError(ctx, err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyInResponse:
			pendingCopyInResponse = false
		case *pgproto3.ErrorResponse:
			pgErr = errorResponseToPgError(msg)
		case *pgproto3.ReadyForQuery:
			<-pgConn.controller
			return commandTag, pgErr
		}
	}

	// Send copy data
	buf = make([]byte, 0, 20000)
	// buf = make([]byte, 0, 65536)
	buf = append(buf, 'd')
	sp := len(buf)
	var readErr error
	signalMessageChan := pgConn.signalMessage()
	for readErr == nil && pgErr == nil {
		var n int
		n, readErr = r.Read(buf[5:cap(buf)])
		if n > 0 {
			buf = buf[0 : n+5]
			pgio.SetInt32(buf[sp:], int32(n+4))

			_, err = pgConn.conn.Write(buf)
			if err != nil {
				pgConn.hardClose()
				<-pgConn.controller

				return "", preferContextOverNetTimeoutError(ctx, err)
			}
		}

		select {
		case <-signalMessageChan:
			msg, err := pgConn.ReceiveMessage()
			if err != nil {
				pgConn.hardClose()
				return "", preferContextOverNetTimeoutError(ctx, err)
			}

			switch msg := msg.(type) {
			case *pgproto3.ErrorResponse:
				pgErr = errorResponseToPgError(msg)
			}
		default:
		}
	}

	buf = buf[:0]
	if readErr == io.EOF || pgErr != nil {
		copyDone := &pgproto3.CopyDone{}
		buf = copyDone.Encode(buf)
	} else {
		copyFail := &pgproto3.CopyFail{Error: readErr.Error()}
		buf = copyFail.Encode(buf)
	}
	_, err = pgConn.conn.Write(buf)
	if err != nil {
		pgConn.hardClose()
		<-pgConn.controller

		return "", preferContextOverNetTimeoutError(ctx, err)
	}

	// Read results
	for {
		msg, err := pgConn.ReceiveMessage()
		if err != nil {
			pgConn.hardClose()
			return "", preferContextOverNetTimeoutError(ctx, err)
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			<-pgConn.controller
			return commandTag, pgErr
		case *pgproto3.CommandComplete:
			commandTag = CommandTag(msg.CommandTag)
		case *pgproto3.ErrorResponse:
			pgErr = errorResponseToPgError(msg)
		}
	}
}

// MultiResultReader is a reader for a command that could return multiple results such as Exec or ExecBatch.
type MultiResultReader struct {
	pgConn                 *PgConn
	ctx                    context.Context
	cleanupContextDeadline func()

	rr *ResultReader

	closed bool
	err    error
}

// ReadAll reads all available results. Calling ReadAll is mutually exclusive with all other MultiResultReader methods.
func (mrr *MultiResultReader) ReadAll() ([]*Result, error) {
	var results []*Result

	for mrr.NextResult() {
		results = append(results, mrr.ResultReader().Read())
	}
	err := mrr.Close()

	return results, err
}

func (mrr *MultiResultReader) receiveMessage() (pgproto3.BackendMessage, error) {
	msg, err := mrr.pgConn.ReceiveMessage()

	if err != nil {
		mrr.cleanupContextDeadline()
		mrr.err = preferContextOverNetTimeoutError(mrr.ctx, err)
		mrr.closed = true
		mrr.pgConn.hardClose()
		return nil, mrr.err
	}

	switch msg := msg.(type) {
	case *pgproto3.ReadyForQuery:
		mrr.cleanupContextDeadline()
		mrr.closed = true
		<-mrr.pgConn.controller
	case *pgproto3.ErrorResponse:
		mrr.err = errorResponseToPgError(msg)
	}

	return msg, nil
}

// NextResult returns advances the MultiResultReader to the next result and returns true if a result is available.
func (mrr *MultiResultReader) NextResult() bool {
	for !mrr.closed && mrr.err == nil {
		msg, err := mrr.receiveMessage()
		if err != nil {
			return false
		}

		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			mrr.rr = &ResultReader{
				pgConn:                 mrr.pgConn,
				multiResultReader:      mrr,
				ctx:                    mrr.ctx,
				cleanupContextDeadline: func() {},
				fieldDescriptions:      msg.Fields,
			}
			return true
		case *pgproto3.CommandComplete:
			mrr.rr = &ResultReader{
				commandTag:       CommandTag(msg.CommandTag),
				commandConcluded: true,
				closed:           true,
			}
			return true
		case *pgproto3.EmptyQueryResponse:
			return false
		}
	}

	return false
}

// ResultReader returns the current ResultReader.
func (mrr *MultiResultReader) ResultReader() *ResultReader {
	return mrr.rr
}

// Close closes the MultiResultReader and returns the first error that occurred during the MultiResultReader's use.
func (mrr *MultiResultReader) Close() error {
	for !mrr.closed {
		_, err := mrr.receiveMessage()
		if err != nil {
			return mrr.err
		}
	}

	return mrr.err
}

// ResultReader is a reader for the result of a single query.
type ResultReader struct {
	pgConn                 *PgConn
	multiResultReader      *MultiResultReader
	ctx                    context.Context
	cleanupContextDeadline func()

	fieldDescriptions []pgproto3.FieldDescription
	rowValues         [][]byte
	commandTag        CommandTag
	commandConcluded  bool
	closed            bool
	err               error
}

// Result is the saved query response that is returned by calling Read on a ResultReader.
type Result struct {
	FieldDescriptions []pgproto3.FieldDescription
	Rows              [][][]byte
	CommandTag        CommandTag
	Err               error
}

// Read saves the query response to a Result.
func (rr *ResultReader) Read() *Result {
	br := &Result{}

	for rr.NextRow() {
		if br.FieldDescriptions == nil {
			br.FieldDescriptions = make([]pgproto3.FieldDescription, len(rr.FieldDescriptions()))
			copy(br.FieldDescriptions, rr.FieldDescriptions())
		}

		row := make([][]byte, len(rr.Values()))
		copy(row, rr.Values())
		br.Rows = append(br.Rows, row)
	}

	br.CommandTag, br.Err = rr.Close()

	return br
}

// NextRow advances the ResultReader to the next row and returns true if a row is available.
func (rr *ResultReader) NextRow() bool {
	for !rr.commandConcluded {
		msg, err := rr.receiveMessage()
		if err != nil {
			return false
		}

		switch msg := msg.(type) {
		case *pgproto3.DataRow:
			rr.rowValues = msg.Values
			return true
		}
	}

	return false
}

// FieldDescriptions returns the field descriptions for the current result set. The returned slice is only valid until
// the ResultReader is closed.
func (rr *ResultReader) FieldDescriptions() []pgproto3.FieldDescription {
	return rr.fieldDescriptions
}

// Values returns the current row data. NextRow must have been previously been called. The returned [][]byte is only
// valid until the next NextRow call or the ResultReader is closed. However, the underlying byte data is safe to
// retain a reference to and mutate.
func (rr *ResultReader) Values() [][]byte {
	return rr.rowValues
}

// Close consumes any remaining result data and returns the command tag or
// error.
func (rr *ResultReader) Close() (CommandTag, error) {
	if rr.closed {
		return rr.commandTag, rr.err
	}
	rr.closed = true

	for !rr.commandConcluded {
		_, err := rr.receiveMessage()
		if err != nil {
			return "", rr.err
		}
	}

	if rr.multiResultReader == nil {
		for {
			msg, err := rr.receiveMessage()
			if err != nil {
				return "", rr.err
			}

			switch msg.(type) {
			case *pgproto3.ReadyForQuery:
				rr.cleanupContextDeadline()
				<-rr.pgConn.controller
				return rr.commandTag, rr.err
			}
		}
	}

	return rr.commandTag, rr.err
}

func (rr *ResultReader) receiveMessage() (msg pgproto3.BackendMessage, err error) {
	if rr.multiResultReader == nil {
		msg, err = rr.pgConn.ReceiveMessage()
	} else {
		msg, err = rr.multiResultReader.receiveMessage()
	}

	if err != nil {
		rr.concludeCommand("", err)
		rr.cleanupContextDeadline()
		rr.closed = true
		if rr.multiResultReader == nil {
			rr.pgConn.hardClose()
		}

		return nil, rr.err
	}

	switch msg := msg.(type) {
	case *pgproto3.RowDescription:
		rr.fieldDescriptions = msg.Fields
	case *pgproto3.CommandComplete:
		rr.concludeCommand(CommandTag(msg.CommandTag), nil)
	case *pgproto3.ErrorResponse:
		rr.concludeCommand("", errorResponseToPgError(msg))
	}

	return msg, nil
}

func (rr *ResultReader) concludeCommand(commandTag CommandTag, err error) {
	if rr.commandConcluded {
		return
	}

	rr.commandTag = commandTag
	rr.err = preferContextOverNetTimeoutError(rr.ctx, err)
	rr.fieldDescriptions = nil
	rr.rowValues = nil
	rr.commandConcluded = true
}

// Batch is a collection of queries that can be sent to the PostgreSQL server in a single round-trip.
type Batch struct {
	buf []byte
}

// ExecParams appends an ExecParams command to the batch. See PgConn.ExecParams for parameter descriptions.
func (batch *Batch) ExecParams(sql string, paramValues [][]byte, paramOIDs []uint32, paramFormats []int16, resultFormats []int16) {
	batch.buf = (&pgproto3.Parse{Query: sql, ParameterOIDs: paramOIDs}).Encode(batch.buf)
	batch.ExecPrepared("", paramValues, paramFormats, resultFormats)
}

// ExecPrepared appends an ExecPrepared e command to the batch. See PgConn.ExecPrepared for parameter descriptions.
func (batch *Batch) ExecPrepared(stmtName string, paramValues [][]byte, paramFormats []int16, resultFormats []int16) {
	batch.buf = (&pgproto3.Bind{PreparedStatement: stmtName, ParameterFormatCodes: paramFormats, Parameters: paramValues, ResultFormatCodes: resultFormats}).Encode(batch.buf)
	batch.buf = (&pgproto3.Describe{ObjectType: 'P'}).Encode(batch.buf)
	batch.buf = (&pgproto3.Execute{}).Encode(batch.buf)
}

// ExecBatch executes all the queries in batch in a single round-trip. Execution is implicitly transactional unless a
// transaction is already in progress or SQL contains transaction control statements.
func (pgConn *PgConn) ExecBatch(ctx context.Context, batch *Batch) *MultiResultReader {
	multiResult := &MultiResultReader{
		pgConn:                 pgConn,
		ctx:                    ctx,
		cleanupContextDeadline: func() {},
	}

	select {
	case <-ctx.Done():
		multiResult.closed = true
		multiResult.err = ctx.Err()
		return multiResult
	case pgConn.controller <- multiResult:
	}
	multiResult.cleanupContextDeadline = contextDoneToConnDeadline(ctx, pgConn.conn)

	batch.buf = (&pgproto3.Sync{}).Encode(batch.buf)
	_, err := pgConn.conn.Write(batch.buf)
	if err != nil {
		pgConn.hardClose()
		multiResult.cleanupContextDeadline()
		multiResult.closed = true
		multiResult.err = preferContextOverNetTimeoutError(ctx, err)
		<-pgConn.controller
		return multiResult
	}

	return multiResult
}

// EscapeString escapes a string such that it can safely be interpolated into a SQL command string. It does not include
// the surrounding single quotes.
//
// The current implementation requires that standard_conforming_strings=on and client_encoding="UTF8". If these
// conditions are not met an error will be returned. It is possible these restrictions will be lifted in the future.
func (pgConn *PgConn) EscapeString(s string) (string, error) {
	if pgConn.ParameterStatus("standard_conforming_strings") != "on" {
		return "", errors.New("EscapeString must be run with standard_conforming_strings=on")
	}

	if pgConn.ParameterStatus("client_encoding") != "UTF8" {
		return "", errors.New("EscapeString must be run with client_encoding=UTF8")
	}

	return strings.Replace(s, "'", "''", -1), nil
}
