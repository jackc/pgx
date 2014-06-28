// Package pgx is a PostgreSQL database driver.
//
// It does not implement the standard database/sql interface.
package pgx

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	log "gopkg.in/inconshreveable/log15.v2"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Transaction isolation levels
const (
	Serializable    = "serializable"
	RepeatableRead  = "repeatable read"
	ReadCommitted   = "read committed"
	ReadUncommitted = "read uncommitted"
)

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	Host       string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port       uint16 // default: 5432
	Database   string
	User       string // default: OS user name
	Password   string
	MsgBufSize int         // Size of work buffer used for transcoding messages. For optimal performance, it should be large enough to store a single row from any result set. Default: 1024
	TLSConfig  *tls.Config // config for TLS connection -- nil disables TLS
	Logger     log.Logger
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	conn               net.Conn      // the underlying TCP or unix domain socket connection
	reader             *bufio.Reader // buffered reader to improve read performance
	wbuf               [1024]byte
	buf                *bytes.Buffer     // work buffer to avoid constant alloc and dealloc
	bufSize            int               // desired size of buf
	Pid                int32             // backend pid
	SecretKey          int32             // key to use to send a cancel query message to the server
	RuntimeParams      map[string]string // parameters that have been reported by the server
	config             ConnConfig        // config used when establishing this connection
	TxStatus           byte
	preparedStatements map[string]*PreparedStatement
	notifications      []*Notification
	alive              bool
	causeOfDeath       error
	logger             log.Logger
	qr                 QueryResult
}

type PreparedStatement struct {
	Name              string
	FieldDescriptions []FieldDescription
	ParameterOids     []Oid
}

type Notification struct {
	Pid     int32  // backend pid that sent the notification
	Channel string // channel from which notification was received
	Payload string
}

type CommandTag string

// RowsAffected returns the number of rows affected. If the CommandTag was not
// for a row affecting command (such as "CREATE TABLE") then it returns 0
func (ct CommandTag) RowsAffected() int64 {
	words := strings.Split(string(ct), " ")
	n, _ := strconv.ParseInt(words[len(words)-1], 10, 64)
	return n
}

// NotSingleRowError is returned when exactly 1 row is expected, but 0 or more than
// 1 row is returned
type NotSingleRowError struct {
	RowCount int64
}

func (e NotSingleRowError) Error() string {
	return fmt.Sprintf("Expected to find 1 row exactly, instead found %d", e.RowCount)
}

// UnexpectedColumnCountError is returned when an unexpected number of columns is
// returned from a Select.
type UnexpectedColumnCountError struct {
	ExpectedCount int16
	ActualCount   int16
}

func (e UnexpectedColumnCountError) Error() string {
	return fmt.Sprintf("Expected result to have %d column(s), instead it has %d", e.ExpectedCount, e.ActualCount)
}

type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}

var NotificationTimeoutError = errors.New("Notification Timeout")
var DeadConnError = errors.New("Connection is dead")

// Connect establishes a connection with a PostgreSQL server using config.
// config.Host must be specified. config.User will default to the OS user name.
// Other config fields are optional.
func Connect(config ConnConfig) (c *Conn, err error) {
	c = new(Conn)

	c.config = config
	if c.config.Logger != nil {
		c.logger = c.config.Logger
	} else {
		c.logger = log.New()
		c.logger.SetHandler(log.DiscardHandler())
	}

	if c.config.User == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		c.config.User = user.Username
		c.logger.Debug("Using default connection config", "User", c.config.User)
	}

	if c.config.Port == 0 {
		c.config.Port = 5432
		c.logger.Debug("Using default connection config", "Port", c.config.Port)
	}
	if c.config.MsgBufSize == 0 {
		c.config.MsgBufSize = 1024
		c.logger.Debug("Using default connection config", "MsgBufSize", c.config.MsgBufSize)
	}

	// See if host is a valid path, if yes connect with a socket
	_, err = os.Stat(c.config.Host)
	if err == nil {
		// For backward compatibility accept socket file paths -- but directories are now preferred
		socket := c.config.Host
		if !strings.Contains(socket, "/.s.PGSQL.") {
			socket = filepath.Join(socket, ".s.PGSQL.") + strconv.FormatInt(int64(c.config.Port), 10)
		}

		c.logger.Info(fmt.Sprintf("Dialing PostgreSQL server at socket: %s", socket))
		c.conn, err = net.Dial("unix", socket)
		if err != nil {
			c.logger.Error(fmt.Sprintf("Connection failed: %v", err))
			return nil, err
		}
	} else {
		c.logger.Info(fmt.Sprintf("Dialing PostgreSQL server at host: %s:%d", c.config.Host, c.config.Port))
		c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.config.Host, c.config.Port))
		if err != nil {
			c.logger.Error(fmt.Sprintf("Connection failed: %v", err))
			return nil, err
		}
	}
	defer func() {
		if c != nil && err != nil {
			c.conn.Close()
			c.alive = false
			c.logger.Error(err.Error())
		}
	}()

	c.bufSize = c.config.MsgBufSize
	c.buf = bytes.NewBuffer(make([]byte, 0, c.bufSize))
	c.RuntimeParams = make(map[string]string)
	c.preparedStatements = make(map[string]*PreparedStatement)
	c.alive = true

	if config.TLSConfig != nil {
		c.logger.Debug("Starting TLS handshake")
		if err = c.startTLS(); err != nil {
			c.logger.Error(fmt.Sprintf("TLS failed: %v", err))
			return
		}
	}

	c.reader = bufio.NewReader(c.conn)

	msg := newStartupMessage()
	msg.options["user"] = c.config.User
	if c.config.Database != "" {
		msg.options["database"] = c.config.Database
	}
	if err = c.txStartupMessage(msg); err != nil {
		return
	}

	for {
		var t byte
		var r *MessageReader
		t, r, err = c.rxMsg()
		if err != nil {
			return nil, err
		}

		switch t {
		case backendKeyData:
			c.rxBackendKeyData(r)
		case authenticationX:
			if err = c.rxAuthenticationX(r); err != nil {
				return nil, err
			}
		case readyForQuery:
			c.rxReadyForQuery(r)
			c.logger = c.logger.New("pid", c.Pid)
			c.logger.Info("Connection established")
			return c, nil
		default:
			if err = c.processContextFreeMsg(t, r); err != nil {
				return nil, err
			}
		}
	}
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close() (err error) {
	if !c.IsAlive() {
		return nil
	}

	err = c.txMsg('X', c.getBuf())
	c.die(errors.New("Closed"))
	c.logger.Info("Closed connection")
	return err
}

// ParseURI parses a database URI into ConnConfig
func ParseURI(uri string) (ConnConfig, error) {
	var cp ConnConfig

	url, err := url.Parse(uri)
	if err != nil {
		return cp, err
	}

	if url.User != nil {
		cp.User = url.User.Username()
		cp.Password, _ = url.User.Password()
	}

	parts := strings.SplitN(url.Host, ":", 2)
	cp.Host = parts[0]
	if len(parts) == 2 {
		p, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return cp, err
		}
		cp.Port = uint16(p)
	}
	cp.Database = strings.TrimLeft(url.Path, "/")

	return cp, nil
}

// SelectValue executes sql and returns a single value. sql can be either a prepared
// statement name or an SQL string. arguments will be sanitized before being
// interpolated into sql strings. arguments should be referenced positionally from
// the sql string as $1, $2, etc.
//
// Returns a UnexpectedColumnCountError if exactly one column is not found
// Returns a NotSingleRowError if exactly one row is not found
func (c *Conn) SelectValue(sql string, arguments ...interface{}) (interface{}, error) {
	startTime := time.Now()

	var numRowsFound int64
	var v interface{}

	qr, _ := c.Query(sql, arguments...)
	defer qr.Close()

	for qr.NextRow() {
		if len(qr.fields) != 1 {
			qr.Close()
			return nil, UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: int16(len(qr.fields))}
		}

		numRowsFound++
		var rr RowReader
		v = rr.ReadValue(qr)
	}
	if qr.Err() != nil {
		return nil, qr.Err()
	}

	if numRowsFound != 1 {
		return nil, NotSingleRowError{RowCount: numRowsFound}
	}

	endTime := time.Now()
	c.logger.Info("SelectValue", "sql", sql, "args", arguments, "rowsFound", numRowsFound, "time", endTime.Sub(startTime))

	return v, nil
}

// SelectValueTo executes sql that returns a single value and writes that value to w.
// No type conversions will be performed. The raw bytes will be written directly to w.
// This is ideal for returning JSON, files, or large text values directly over HTTP.

// sql can be either a prepared statement name or an SQL string. arguments will be
// sanitized before being interpolated into sql strings. arguments should be
// referenced positionally from the sql string as $1, $2, etc.
//
// Returns a UnexpectedColumnCountError if exactly one column is not found
// Returns a NotSingleRowError if exactly one row is not found
func (c *Conn) SelectValueTo(w io.Writer, sql string, arguments ...interface{}) (err error) {
	startTime := time.Now()

	defer func() {
		if err == nil {
			endTime := time.Now()
			c.logger.Info("SelectValueTo", "sql", sql, "args", arguments, "time", endTime.Sub(startTime))
		} else {
			c.logger.Error(fmt.Sprintf("SelectValueTo `%s` with %v failed: %v", sql, arguments, err))
		}
	}()

	err = c.sendQuery(sql, arguments...)
	if err != nil {
		return err
	}

	var numRowsFound int64
	var softErr error

	for {
		var t byte
		var bodySize int32

		t, bodySize, err = c.rxMsgHeader()
		if err != nil {
			return err
		}

		if t == dataRow {
			numRowsFound++

			if numRowsFound > 1 {
				softErr = NotSingleRowError{RowCount: numRowsFound}
			}

			if softErr != nil {
				c.rxMsgBody(bodySize) // Read and discard rest of message
				continue
			}

			softErr = c.rxDataRowValueTo(w, bodySize)
		} else {
			var body *bytes.Buffer
			body, err = c.rxMsgBody(bodySize)
			if err != nil {
				return err
			}

			r := (*MessageReader)(body)
			switch t {
			case readyForQuery:
				c.rxReadyForQuery(r)
				return softErr
			case rowDescription:
			case commandComplete:
			case bindComplete:
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && softErr == nil {
					softErr = e
				}
			}
		}
	}
}

func (c *Conn) rxDataRowValueTo(w io.Writer, bodySize int32) (err error) {
	b := make([]byte, 2)
	_, err = io.ReadFull(c.reader, b)
	if err != nil {
		c.die(err)
		return
	}
	columnCount := int16(binary.BigEndian.Uint16(b))

	if columnCount != 1 {
		// Read the rest of the data row so it can be discarded
		if _, err = io.CopyN(ioutil.Discard, c.reader, int64(bodySize-2)); err != nil {
			c.die(err)
			return
		}
		err = UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: columnCount}
		return
	}

	b = make([]byte, 4)
	_, err = io.ReadFull(c.reader, b)
	if err != nil {
		c.die(err)
		return
	}
	valueSize := int32(binary.BigEndian.Uint32(b))

	if valueSize == -1 {
		err = errors.New("SelectValueTo cannot handle null")
		return
	}

	_, err = io.CopyN(w, c.reader, int64(valueSize))
	if err != nil {
		c.die(err)
	}

	return
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
func (c *Conn) Prepare(name, sql string) (ps *PreparedStatement, err error) {
	defer func() {
		if err != nil {
			c.logger.Error(fmt.Sprintf("Prepare `%s` as `%s` failed: %v", name, sql, err))
		}
	}()

	// parse
	buf := c.getBuf()
	buf.WriteString(name)
	buf.WriteByte(0)
	buf.WriteString(sql)
	buf.WriteByte(0)
	binary.Write(buf, binary.BigEndian, int16(0))
	err = c.txMsg('P', buf)
	if err != nil {
		return nil, err
	}

	// describe
	buf = c.getBuf()
	buf.WriteByte('S')
	buf.WriteString(name)
	buf.WriteByte(0)
	err = c.txMsg('D', buf)
	if err != nil {
		return nil, err
	}

	// sync
	err = c.txMsg('S', c.getBuf())
	if err != nil {
		return nil, err
	}

	ps = &PreparedStatement{Name: name}

	var softErr error

	for {
		var t byte
		var r *MessageReader
		t, r, err := c.rxMsg()
		if err != nil {
			return nil, err
		}

		switch t {
		case parseComplete:
		case parameterDescription:
			ps.ParameterOids = c.rxParameterDescription(r)
		case rowDescription:
			ps.FieldDescriptions = c.rxRowDescription(r)
			for i := range ps.FieldDescriptions {
				oid := ps.FieldDescriptions[i].DataType
				vt := ValueTranscoders[oid]

				if vt != nil {
					ps.FieldDescriptions[i].FormatCode = vt.DecodeFormat
				}
			}
		case noData:
		case readyForQuery:
			c.rxReadyForQuery(r)
			c.preparedStatements[name] = ps
			return ps, softErr
		default:
			if e := c.processContextFreeMsg(t, r); e != nil && softErr == nil {
				softErr = e
			}
		}
	}
}

// Deallocate released a prepared statement
func (c *Conn) Deallocate(name string) (err error) {
	delete(c.preparedStatements, name)
	_, err = c.Exec("deallocate " + c.QuoteIdentifier(name))
	return
}

// Listen establishes a PostgreSQL listen/notify to channel
func (c *Conn) Listen(channel string) (err error) {
	_, err = c.Exec("listen " + channel)
	return
}

// WaitForNotification waits for a PostgreSQL notification for up to timeout.
// If the timeout occurs it returns pgx.NotificationTimeoutError
func (c *Conn) WaitForNotification(timeout time.Duration) (*Notification, error) {
	if len(c.notifications) > 0 {
		notification := c.notifications[0]
		c.notifications = c.notifications[1:]
		return notification, nil
	}

	var zeroTime time.Time
	stopTime := time.Now().Add(timeout)

	for {
		// Use SetReadDeadline to implement the timeout. SetReadDeadline will
		// cause operations to fail with a *net.OpError that has a Timeout()
		// of true. Because the normal pgx rxMsg path considers any error to
		// have potentially corrupted the state of the connection, it dies
		// on any errors. So to avoid timeout errors in rxMsg we set the
		// deadline and peek into the reader. If a timeout error occurs there
		// we don't break the pgx connection. If the Peek returns that data
		// is available then we turn off the read deadline before the rxMsg.
		err := c.conn.SetReadDeadline(stopTime)
		if err != nil {
			return nil, err
		}

		// Wait until there is a byte available before continuing onto the normal msg reading path
		_, err = c.reader.Peek(1)
		if err != nil {
			c.conn.SetReadDeadline(zeroTime) // we can only return one error and we already have one -- so ignore possiple error from SetReadDeadline
			if err, ok := err.(*net.OpError); ok && err.Timeout() {
				return nil, NotificationTimeoutError
			}
			return nil, err
		}

		err = c.conn.SetReadDeadline(zeroTime)
		if err != nil {
			return nil, err
		}

		var t byte
		var r *MessageReader
		if t, r, err = c.rxMsg(); err == nil {
			if err = c.processContextFreeMsg(t, r); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}

		if len(c.notifications) > 0 {
			notification := c.notifications[0]
			c.notifications = c.notifications[1:]
			return notification, nil
		}
	}
}

func (c *Conn) IsAlive() bool {
	return c.alive
}

func (c *Conn) CauseOfDeath() error {
	return c.causeOfDeath
}

type RowReader struct{}

// TODO - Read*...

func (rr *RowReader) ReadInt32(qr *QueryResult) int32 {
	fd, size := qr.NextColumn()

	// TODO - do something about nulls
	if size == -1 {
		panic("Can't handle nulls")
	}

	return decodeInt4(qr, fd, size)
}

func (rr *RowReader) ReadTime(qr *QueryResult) time.Time {
	fd, size := qr.NextColumn()

	// TODO - do something about nulls
	if size == -1 {
		panic("Can't handle nulls")
	}

	return decodeTimestampTz(qr, fd, size)
}

func (rr *RowReader) ReadDate(qr *QueryResult) time.Time {
	fd, size := qr.NextColumn()

	// TODO - do something about nulls
	if size == -1 {
		panic("Can't handle nulls")
	}

	return decodeDate(qr, fd, size)
}

func (rr *RowReader) ReadString(qr *QueryResult) string {
	_, size := qr.NextColumn()
	return qr.mr.ReadString(size)
}

func (rr *RowReader) ReadValue(qr *QueryResult) interface{} {
	fd, size := qr.NextColumn()

	if size > -1 {
		if vt, present := ValueTranscoders[fd.DataType]; present && vt.Decode != nil {
			return vt.Decode(qr, fd, size)
		} else {
			return qr.mr.ReadString(size)
		}
	} else {
		return nil
	}
}

type QueryResult struct {
	pool      *ConnPool
	conn      *Conn
	mr        *MessageReader
	fields    []FieldDescription
	rowCount  int
	columnIdx int
	err       error
	closed    bool
}

func (qr *QueryResult) FieldDescriptions() []FieldDescription {
	return qr.fields
}

func (qr *QueryResult) MessageReader() *MessageReader {
	return qr.mr
}

func (qr *QueryResult) close() {
	if qr.pool != nil {
		qr.pool.Release(qr.conn)
		qr.pool = nil
	}

	qr.closed = true
}

func (qr *QueryResult) readUntilReadyForQuery() {
	for {
		t, r, err := qr.conn.rxMsg()
		if err != nil {
			qr.close()
			return
		}

		switch t {
		case readyForQuery:
			qr.conn.rxReadyForQuery(r)
			qr.close()
			return
		case rowDescription:
		case dataRow:
		case commandComplete:
		case bindComplete:
		default:
			err = qr.conn.processContextFreeMsg(t, r)
			if err != nil {
				qr.close()
				return
			}
		}
	}
}

func (qr *QueryResult) Close() {
	if qr.closed {
		return
	}
	qr.readUntilReadyForQuery()
	qr.close()
}

func (qr *QueryResult) Err() error {
	return qr.err
}

func (qr *QueryResult) Fatal(err error) {
	qr.err = err
	qr.Close()
}

func (qr *QueryResult) NextRow() bool {
	if qr.closed {
		return false
	}

	qr.rowCount++
	qr.columnIdx = 0

	for {
		t, r, err := qr.conn.rxMsg()
		if err != nil {
			qr.Fatal(err)
			return false
		}

		switch t {
		case readyForQuery:
			qr.conn.rxReadyForQuery(r)
			qr.close()
			return false
		case dataRow:
			fieldCount := int(r.ReadInt16())
			if fieldCount != len(qr.fields) {
				qr.Fatal(ProtocolError(fmt.Sprintf("Row description field count (%v) and data row field count (%v) do not match", len(qr.fields), fieldCount)))
				return false
			}

			qr.mr = r
			return true
		case commandComplete:
		case bindComplete:
		default:
			err = qr.conn.processContextFreeMsg(t, r)
			if err != nil {
				qr.Fatal(err)
				return false
			}
		}
	}
}

func (qr *QueryResult) NextColumn() (*FieldDescription, int32) {
	fd := &qr.fields[qr.columnIdx]
	qr.columnIdx++
	size := qr.mr.ReadInt32()

	return fd, size
}

// TODO - document
func (c *Conn) Query(sql string, args ...interface{}) (*QueryResult, error) {
	c.qr = QueryResult{conn: c}
	qr := &c.qr

	// TODO - shouldn't be messing with qr.err and qr.closed directly
	if ps, present := c.preparedStatements[sql]; present {
		qr.fields = ps.FieldDescriptions
		qr.err = c.sendPreparedQuery(ps, args...)
		if qr.err != nil {
			qr.closed = true
		}
		return qr, qr.err
	}

	qr.err = c.sendSimpleQuery(sql, args...)
	if qr.err != nil {
		qr.closed = true
		return qr, qr.err
	}

	// Simple queries don't know the field descriptions of the result.
	// Read until that is known before returning
	for {
		t, r, err := c.rxMsg()
		if err != nil {
			qr.err = err
			qr.closed = true
			return qr, qr.err
		}

		switch t {
		case rowDescription:
			qr.fields = qr.conn.rxRowDescription(r)
			return qr, nil
		default:
			err = qr.conn.processContextFreeMsg(t, r)
			if err != nil {
				qr.closed = true
				qr.err = err
				return qr, qr.err
			}
		}
	}
}

func (c *Conn) sendQuery(sql string, arguments ...interface{}) (err error) {
	if ps, present := c.preparedStatements[sql]; present {
		return c.sendPreparedQuery(ps, arguments...)
	} else {
		return c.sendSimpleQuery(sql, arguments...)
	}
}

func (c *Conn) sendSimpleQuery(sql string, arguments ...interface{}) (err error) {
	if len(arguments) > 0 {
		sql, err = c.SanitizeSql(sql, arguments...)
		if err != nil {
			return
		}
	}

	buf := c.getBuf()

	_, err = buf.WriteString(sql)
	if err != nil {
		return
	}
	err = buf.WriteByte(0)
	if err != nil {
		return
	}

	return c.txMsg('Q', buf)
}

func (c *Conn) sendPreparedQuery(ps *PreparedStatement, arguments ...interface{}) (err error) {
	if len(ps.ParameterOids) != len(arguments) {
		return fmt.Errorf("Prepared statement \"%v\" requires %d parameters, but %d were provided", ps.Name, len(ps.ParameterOids), len(arguments))
	}

	// bind
	wbuf := newWriteBuf(c.wbuf[0:0], 'B')
	wbuf.WriteByte(0)
	wbuf.WriteCString(ps.Name)

	wbuf.WriteInt16(int16(len(ps.ParameterOids)))
	for _, oid := range ps.ParameterOids {
		transcoder := ValueTranscoders[oid]
		if transcoder == nil {
			transcoder = defaultTranscoder
		}
		wbuf.WriteInt16(transcoder.EncodeFormat)
	}

	wbuf.WriteInt16(int16(len(arguments)))
	for i, oid := range ps.ParameterOids {
		if arguments[i] != nil {
			transcoder := ValueTranscoders[oid]
			if transcoder == nil {
				transcoder = defaultTranscoder
			}
			err = transcoder.EncodeTo(wbuf, arguments[i])
			if err != nil {
				return err
			}
		} else {
			wbuf.WriteInt32(int32(-1))
		}
	}

	wbuf.WriteInt16(int16(len(ps.FieldDescriptions)))
	for _, fd := range ps.FieldDescriptions {
		transcoder := ValueTranscoders[fd.DataType]
		if transcoder != nil {
			wbuf.WriteInt16(transcoder.DecodeFormat)
		} else {
			wbuf.WriteInt16(0)
		}
	}

	// execute
	wbuf.startMsg('E')
	wbuf.WriteByte(0)
	wbuf.WriteInt32(0)

	// sync
	wbuf.startMsg('S')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)

	return err
}

// Exec executes sql. sql can be either a prepared statement name or an SQL string.
// arguments will be sanitized before being interpolated into sql strings. arguments
// should be referenced positionally from the sql string as $1, $2, etc.
func (c *Conn) Exec(sql string, arguments ...interface{}) (commandTag CommandTag, err error) {
	startTime := time.Now()

	defer func() {
		if err == nil {
			endTime := time.Now()
			c.logger.Info("Exec", "sql", sql, "args", arguments, "time", endTime.Sub(startTime))
		} else {
			c.logger.Error("Exec", "sql", sql, "args", arguments, "error", err)
		}
	}()

	if err = c.sendQuery(sql, arguments...); err != nil {
		return
	}

	var softErr error

	for {
		var t byte
		var r *MessageReader
		t, r, err = c.rxMsg()
		if err != nil {
			return commandTag, err
		}

		switch t {
		case readyForQuery:
			c.rxReadyForQuery(r)
			return commandTag, softErr
		case rowDescription:
		case dataRow:
		case bindComplete:
		case commandComplete:
			commandTag = CommandTag(r.ReadCString())
		default:
			if e := c.processContextFreeMsg(t, r); e != nil && softErr == nil {
				softErr = e
			}
		}
	}
}

// Transaction runs f in a transaction. f should return true if the transaction
// should be committed or false if it should be rolled back. Return value committed
// is if the transaction was committed or not. committed should be checked separately
// from err as an explicit rollback is not an error. Transaction will use the default
// isolation level for the current connection. To use a specific isolation level see
// TransactionIso
func (c *Conn) Transaction(f func() bool) (committed bool, err error) {
	return c.transaction("", f)
}

// TransactionIso is the same as Transaction except it takes an isoLevel argument that
// it uses as the transaction isolation level.
//
// Valid isolation levels (and their constants) are:
//   serializable (pgx.Serializable)
//   repeatable read (pgx.RepeatableRead)
//   read committed (pgx.ReadCommitted)
//   read uncommitted (pgx.ReadUncommitted)
func (c *Conn) TransactionIso(isoLevel string, f func() bool) (committed bool, err error) {
	return c.transaction(isoLevel, f)
}

func (c *Conn) transaction(isoLevel string, f func() bool) (committed bool, err error) {
	var beginSql string
	if isoLevel == "" {
		beginSql = "begin"
	} else {
		beginSql = fmt.Sprintf("begin isolation level %s", isoLevel)
	}

	if _, err = c.Exec(beginSql); err != nil {
		return
	}
	defer func() {
		if committed && c.TxStatus == 'T' {
			_, err = c.Exec("commit")
			if err != nil {
				committed = false
			}
		} else {
			_, err = c.Exec("rollback")
			committed = false
		}
	}()

	committed = f()
	return
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Conn) processContextFreeMsg(t byte, r *MessageReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	case noticeResponse:
		return nil
	case notificationResponse:
		return c.rxNotificationResponse(r)
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}
}

func (c *Conn) rxMsg() (t byte, r *MessageReader, err error) {
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return
	}

	var body *bytes.Buffer
	if body, err = c.rxMsgBody(bodySize); err != nil {
		return
	}
	r = (*MessageReader)(body)
	return
}

func (c *Conn) rxMsgHeader() (t byte, bodySize int32, err error) {
	if !c.alive {
		return 0, 0, DeadConnError
	}

	t, err = c.reader.ReadByte()
	if err != nil {
		c.die(err)
		return 0, 0, err
	}

	b := make([]byte, 4)
	_, err = io.ReadFull(c.reader, b)
	if err != nil {
		c.die(err)
		return 0, 0, err
	}

	bodySize = int32(binary.BigEndian.Uint32(b))

	bodySize -= 4 // remove self from size
	return t, bodySize, err
}

func (c *Conn) rxMsgBody(bodySize int32) (*bytes.Buffer, error) {
	if !c.alive {
		return nil, DeadConnError
	}

	buf := c.getBuf()
	_, err := io.CopyN(buf, c.reader, int64(bodySize))
	if err != nil {
		c.die(err)
		return nil, err
	}

	return buf, nil
}

func (c *Conn) rxAuthenticationX(r *MessageReader) (err error) {
	code := r.ReadInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		err = c.txPasswordMessage(c.config.Password)
	case 5: // AuthenticationMD5Password
		salt := r.ReadString(4)
		digestedPassword := "md5" + hexMD5(hexMD5(c.config.Password+c.config.User)+salt)
		err = c.txPasswordMessage(digestedPassword)
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Conn) rxParameterStatus(r *MessageReader) {
	key := r.ReadCString()
	value := r.ReadCString()
	c.RuntimeParams[key] = value
}

func (c *Conn) rxErrorResponse(r *MessageReader) (err PgError) {
	for {
		switch r.ReadByte() {
		case 'S':
			err.Severity = r.ReadCString()
		case 'C':
			err.Code = r.ReadCString()
		case 'M':
			err.Message = r.ReadCString()
		case 0: // End of error message
			if err.Severity == "FATAL" {
				c.die(err)
			}
			return
		default: // Ignore other error fields
			r.ReadCString()
		}
	}
}

func (c *Conn) rxBackendKeyData(r *MessageReader) {
	c.Pid = r.ReadInt32()
	c.SecretKey = r.ReadInt32()
}

func (c *Conn) rxReadyForQuery(r *MessageReader) {
	c.TxStatus = r.ReadByte()
}

func (c *Conn) rxRowDescription(r *MessageReader) (fields []FieldDescription) {
	fieldCount := r.ReadInt16()
	fields = make([]FieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.Name = r.ReadCString()
		f.Table = r.ReadOid()
		f.AttributeNumber = r.ReadInt16()
		f.DataType = r.ReadOid()
		f.DataTypeSize = r.ReadInt16()
		f.Modifier = r.ReadInt32()
		f.FormatCode = r.ReadInt16()
	}
	return
}

func (c *Conn) rxParameterDescription(r *MessageReader) (parameters []Oid) {
	parameterCount := r.ReadInt16()
	parameters = make([]Oid, 0, parameterCount)
	for i := int16(0); i < parameterCount; i++ {
		parameters = append(parameters, r.ReadOid())
	}
	return
}

func (c *Conn) rxCommandComplete(r *MessageReader) string {
	return r.ReadCString()
}

func (c *Conn) rxNotificationResponse(r *MessageReader) (err error) {
	n := new(Notification)
	n.Pid = r.ReadInt32()
	n.Channel = r.ReadCString()
	n.Payload = r.ReadCString()
	c.notifications = append(c.notifications, n)
	return
}

func (c *Conn) startTLS() (err error) {
	err = binary.Write(c.conn, binary.BigEndian, []int32{8, 80877103})
	if err != nil {
		return
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(c.conn, response); err != nil {
		return
	}

	if response[0] != 'S' {
		err = errors.New("Could not use TLS")
		return
	}

	c.conn = tls.Client(c.conn, c.config.TLSConfig)

	return nil
}

func (c *Conn) txStartupMessage(msg *startupMessage) error {
	_, err := c.conn.Write(msg.Bytes())
	return err
}

func (c *Conn) txMsg(identifier byte, buf *bytes.Buffer) (err error) {
	if !c.alive {
		return DeadConnError
	}

	defer func() {
		if err != nil {
			c.die(err)
		}
	}()

	err = binary.Write(c.conn, binary.BigEndian, identifier)
	if err != nil {
		return
	}

	err = binary.Write(c.conn, binary.BigEndian, int32(buf.Len()+4))
	if err != nil {
		return
	}

	_, err = buf.WriteTo(c.conn)
	if err != nil {
		return
	}

	return
}

func (c *Conn) txPasswordMessage(password string) (err error) {
	buf := c.getBuf()

	_, err = buf.WriteString(password)
	if err != nil {
		return
	}
	buf.WriteByte(0)
	if err != nil {
		return
	}
	err = c.txMsg('p', buf)
	return
}

// Gets the shared connection buffer. Since bytes.Buffer never releases memory from
// its internal byte array, check on the size and create a new bytes.Buffer so the
// old one can get GC'ed
func (c *Conn) getBuf() *bytes.Buffer {
	c.buf.Reset()
	if cap(c.buf.Bytes()) > c.bufSize {
		c.logger.Debug(fmt.Sprintf("c.buf (%d) is larger than c.bufSize (%d) -- resetting", cap(c.buf.Bytes()), c.bufSize))
		c.buf = bytes.NewBuffer(make([]byte, 0, c.bufSize))
	}
	return c.buf
}

func (c *Conn) die(err error) {
	c.alive = false
	c.causeOfDeath = err
	c.conn.Close()
}
