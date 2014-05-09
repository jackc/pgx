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
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ConnectionParameters contains all the options used to establish a connection.
type ConnectionParameters struct {
	Socket     string // path to unix domain socket directory (e.g. /private/tmp)
	Host       string // url (e.g. localhost)
	Port       uint16 // default: 5432
	Database   string
	User       string // default: OS user name
	Password   string
	MsgBufSize int         // Size of work buffer used for transcoding messages. For optimal performance, it should be large enough to store a single row from any result set. Default: 1024
	TLSConfig  *tls.Config // config for TLS connection -- nil disables TLS
	Logger     Logger
}

// Connection is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnectionPool to manage access to multiple database connections from multiple
// goroutines.
type Connection struct {
	conn               net.Conn             // the underlying TCP or unix domain socket connection
	reader             *bufio.Reader        // buffered reader to improve read performance
	writer             *bufio.Writer        // buffered writer to avoid sending tiny packets
	buf                *bytes.Buffer        // work buffer to avoid constant alloc and dealloc
	bufSize            int                  // desired size of buf
	Pid                int32                // backend pid
	SecretKey          int32                // key to use to send a cancel query message to the server
	RuntimeParams      map[string]string    // parameters that have been reported by the server
	parameters         ConnectionParameters // parameters used when establishing this connection
	TxStatus           byte
	preparedStatements map[string]*preparedStatement
	notifications      []*Notification
	alive              bool
	causeOfDeath       error
	logger             Logger
}

type preparedStatement struct {
	Name              string
	FieldDescriptions []FieldDescription
	ParameterOids     []Oid
}

type Notification struct {
	Pid     int32  // backend pid that sent the notification
	Channel string // channel from which notification was received
	Payload string
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

// Connect establishes a connection with a PostgreSQL server using parameters. One
// of parameters.Socket or parameters.Host must be specified. parameters.User
// will default to the OS user name. Other parameters fields are optional.
func Connect(parameters ConnectionParameters) (c *Connection, err error) {
	c = new(Connection)

	c.parameters = parameters
	if c.parameters.Logger != nil {
		c.logger = c.parameters.Logger
	} else {
		c.logger = nullLogger("null")
	}

	if c.parameters.User == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		c.logger.Debug("Using default User " + user.Username)
		c.parameters.User = user.Username
	}

	if c.parameters.Port == 0 {
		c.logger.Debug("Using default Port")
		c.parameters.Port = 5432
	}
	if c.parameters.MsgBufSize == 0 {
		c.logger.Debug("Using default MsgBufSize")
		c.parameters.MsgBufSize = 1024
	}

	if c.parameters.Socket != "" {
		// For backward compatibility accept socket file paths -- but directories are now preferred
		socket := c.parameters.Socket
		if !strings.Contains(socket, "/.s.PGSQL.") {
			socket = filepath.Join(socket, ".s.PGSQL.") + strconv.FormatInt(int64(c.parameters.Port), 10)
		}

		c.logger.Info(fmt.Sprintf("Dialing PostgreSQL server at socket: %s", socket))
		c.conn, err = net.Dial("unix", socket)
		if err != nil {
			c.logger.Error(fmt.Sprintf("Connection failed: %v", err))
			return nil, err
		}
	} else if c.parameters.Host != "" {
		c.logger.Info(fmt.Sprintf("Dialing PostgreSQL server at host: %s:%d", c.parameters.Host, c.parameters.Port))
		c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.parameters.Host, c.parameters.Port))
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

	c.bufSize = c.parameters.MsgBufSize
	c.buf = bytes.NewBuffer(make([]byte, 0, c.bufSize))
	c.RuntimeParams = make(map[string]string)
	c.preparedStatements = make(map[string]*preparedStatement)
	c.alive = true

	if parameters.TLSConfig != nil {
		c.logger.Debug("Starting TLS handshake")
		if err = c.startTLS(); err != nil {
			c.logger.Error(fmt.Sprintf("TLS failed: %v", err))
			return
		}
	}

	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)

	msg := newStartupMessage()
	msg.options["user"] = c.parameters.User
	if c.parameters.Database != "" {
		msg.options["database"] = c.parameters.Database
	}
	if err = c.txStartupMessage(msg); err != nil {
		return
	}

	for {
		var t byte
		var r *MessageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case backendKeyData:
				c.rxBackendKeyData(r)
			case authenticationX:
				if err = c.rxAuthenticationX(r); err != nil {
					return nil, err
				}
			case readyForQuery:
				c.rxReadyForQuery(r)
				c.logger = newPidLogger(c.Pid, c.logger)
				c.logger.Info("Connection established")
				return c, nil
			default:
				if err = c.processContextFreeMsg(t, r); err != nil {
					return nil, err
				}
			}
		} else {
			return nil, err
		}
	}
}

func (c *Connection) Close() (err error) {
	err = c.txMsg('X', c.getBuf(), true)
	c.die(errors.New("Closed"))
	c.logger.Info("Closed connection")
	return err
}

// ParseURI parses a database URI into ConnectionParameters
func ParseURI(uri string) (ConnectionParameters, error) {
	var cp ConnectionParameters

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

// SelectFunc executes sql and for each row returned calls onDataRow. sql can be
// either a prepared statement name or an SQL string. arguments will be sanitized
// before being interpolated into sql strings. arguments should be referenced
// positionally from the sql string as $1, $2, etc.
//
// SelectFunc calls onDataRow as the rows are received. This means that it does not
// need to simultaneously store the entire result set in memory. It also means that
// it is possible to process some rows and then for an error to occur. Callers
// should be aware of this possibility.
func (c *Connection) SelectFunc(sql string, onDataRow func(*DataRowReader) error, arguments ...interface{}) (err error) {
	defer func() {
		if err != nil {
			c.logger.Error(fmt.Sprintf("SelectFunc `%s` with %v failed: %v", sql, arguments, err))
		}
	}()

	var fields []FieldDescription

	if ps, present := c.preparedStatements[sql]; present {
		fields = ps.FieldDescriptions
		err = c.sendPreparedQuery(ps, arguments...)
	} else {
		err = c.sendSimpleQuery(sql, arguments...)
	}
	if err != nil {
		return
	}

	for {
		if t, r, rxErr := c.rxMsg(); rxErr == nil {
			switch t {
			case readyForQuery:
				c.rxReadyForQuery(r)
				return
			case rowDescription:
				fields = c.rxRowDescription(r)
			case dataRow:
				if err == nil {
					var drr *DataRowReader
					drr, err = newDataRowReader(r, fields)
					if err == nil {
						err = onDataRow(drr)
					}
				}
			case commandComplete:
			case bindComplete:
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
					err = e
				}
			}
		} else {
			return rxErr
		}
	}
}

// SelectRows executes sql and returns a slice of maps representing the found rows.
// sql can be either a prepared statement name or an SQL string. arguments will be
// sanitized before being interpolated into sql strings. arguments should be referenced
// positionally from the sql string as $1, $2, etc.
func (c *Connection) SelectRows(sql string, arguments ...interface{}) (rows []map[string]interface{}, err error) {
	rows = make([]map[string]interface{}, 0, 8)
	onDataRow := func(r *DataRowReader) error {
		rows = append(rows, c.rxDataRow(r))
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	return
}

// SelectRow executes sql and returns a map representing the found row.
// sql can be either a prepared statement name or an SQL string. arguments will be
// sanitized before being interpolated into sql strings. arguments should be referenced
// positionally from the sql string as $1, $2, etc.
//
// Returns a NotSingleRowError if exactly one row is not found
func (c *Connection) SelectRow(sql string, arguments ...interface{}) (row map[string]interface{}, err error) {
	var numRowsFound int64

	onDataRow := func(r *DataRowReader) error {
		numRowsFound++
		row = c.rxDataRow(r)
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	if err == nil && numRowsFound != 1 {
		err = NotSingleRowError{RowCount: numRowsFound}
	}
	return
}

// SelectValue executes sql and returns a single value. sql can be either a prepared
// statement name or an SQL string. arguments will be sanitized before being
// interpolated into sql strings. arguments should be referenced positionally from
// the sql string as $1, $2, etc.
//
// Returns a UnexpectedColumnCountError if exactly one column is not found
// Returns a NotSingleRowError if exactly one row is not found
func (c *Connection) SelectValue(sql string, arguments ...interface{}) (v interface{}, err error) {
	var numRowsFound int64

	onDataRow := func(r *DataRowReader) error {
		if len(r.fields) != 1 {
			return UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: int16(len(r.fields))}
		}

		numRowsFound++
		v = r.ReadValue()
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	if err == nil {
		if numRowsFound != 1 {
			err = NotSingleRowError{RowCount: numRowsFound}
		}
	}
	return
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
func (c *Connection) SelectValueTo(w io.Writer, sql string, arguments ...interface{}) (err error) {
	defer func() {
		if err != nil {
			c.logger.Error(fmt.Sprintf("SelectValueTo `%s` with %v failed: %v", sql, arguments, err))
		}
	}()

	if err = c.sendQuery(sql, arguments...); err != nil {
		return
	}

	var numRowsFound int64

	for {
		if t, bodySize, rxErr := c.rxMsgHeader(); rxErr == nil {
			if t == dataRow {
				numRowsFound++

				if numRowsFound > 1 {
					err = NotSingleRowError{RowCount: numRowsFound}
				}

				if err != nil {
					c.rxMsgBody(bodySize) // Read and discard rest of message
					continue
				}

				err = c.rxDataRowValueTo(w, bodySize)
			} else {
				var body *bytes.Buffer
				if body, rxErr = c.rxMsgBody(bodySize); rxErr == nil {
					r := newMessageReader(body)
					switch t {
					case readyForQuery:
						c.rxReadyForQuery(r)
						return
					case rowDescription:
					case commandComplete:
					case bindComplete:
					default:
						if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
							err = e
						}
					}
				} else {
					return rxErr
				}
			}
		} else {
			return rxErr
		}
	}
	return
}

func (c *Connection) rxDataRowValueTo(w io.Writer, bodySize int32) (err error) {
	var columnCount int16
	err = binary.Read(c.reader, binary.BigEndian, &columnCount)
	if err != nil {
		c.die(err)
		return
	}

	if columnCount != 1 {
		// Read the rest of the data row so it can be discarded
		if _, err = io.CopyN(ioutil.Discard, c.reader, int64(bodySize-2)); err != nil {
			c.die(err)
			return
		}
		err = UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: columnCount}
		return
	}

	var valueSize int32
	err = binary.Read(c.reader, binary.BigEndian, &valueSize)
	if err != nil {
		c.die(err)
		return
	}

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

// SelectValues executes sql and returns a slice of values. sql can be either a prepared
// statement name or an SQL string. arguments will be sanitized before being
// interpolated into sql strings. arguments should be referenced positionally from
// the sql string as $1, $2, etc.
//
// Returns a UnexpectedColumnCountError if exactly one column is not found
func (c *Connection) SelectValues(sql string, arguments ...interface{}) (values []interface{}, err error) {
	values = make([]interface{}, 0, 8)
	onDataRow := func(r *DataRowReader) error {
		if len(r.fields) != 1 {
			return UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: int16(len(r.fields))}
		}

		values = append(values, r.ReadValue())
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	return
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
func (c *Connection) Prepare(name, sql string) (err error) {
	defer func() {
		if err != nil {
			c.logger.Error(fmt.Sprintf("Prepare `%s` as `%s` failed: %v", name, sql, err))
		}
	}()

	// parse
	buf := c.getBuf()
	w := newMessageWriter(buf)
	w.WriteCString(name)
	w.WriteCString(sql)
	w.Write(int16(0))
	if w.Err != nil {
		return w.Err
	}
	err = c.txMsg('P', buf, false)
	if err != nil {
		return
	}

	// describe
	buf = c.getBuf()
	w = newMessageWriter(buf)
	w.WriteByte('S')
	w.WriteCString(name)
	if w.Err != nil {
		return w.Err
	}

	err = c.txMsg('D', buf, false)
	if err != nil {
		return
	}

	// sync
	err = c.txMsg('S', c.getBuf(), true)
	if err != nil {
		return err
	}

	ps := preparedStatement{Name: name}

	for {
		if t, r, rxErr := c.rxMsg(); rxErr == nil {
			switch t {
			case parseComplete:
			case parameterDescription:
				ps.ParameterOids = c.rxParameterDescription(r)
			case rowDescription:
				ps.FieldDescriptions = c.rxRowDescription(r)
				for i := range ps.FieldDescriptions {
					oid := ps.FieldDescriptions[i].DataType
					if ValueTranscoders[oid] != nil && ValueTranscoders[oid].DecodeBinary != nil {
						ps.FieldDescriptions[i].FormatCode = 1
					}
				}
			case noData:
			case readyForQuery:
				c.rxReadyForQuery(r)
				c.preparedStatements[name] = &ps
				return
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
					err = e
				}
			}
		} else {
			return rxErr
		}
	}
}

// Deallocate released a prepared statement
func (c *Connection) Deallocate(name string) (err error) {
	delete(c.preparedStatements, name)
	_, err = c.Execute("deallocate " + c.QuoteIdentifier(name))
	return
}

// Listen establishes a PostgreSQL listen/notify to channel
func (c *Connection) Listen(channel string) (err error) {
	_, err = c.Execute("listen " + channel)
	return
}

// WaitForNotification waits for a PostgreSQL notification for up to timeout.
// If the timeout occurs it returns pgx.NotificationTimeoutError
func (c *Connection) WaitForNotification(timeout time.Duration) (*Notification, error) {
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

func (c *Connection) IsAlive() bool {
	return c.alive
}

func (c *Connection) CauseOfDeath() error {
	return c.causeOfDeath
}

func (c *Connection) sendQuery(sql string, arguments ...interface{}) (err error) {
	if ps, present := c.preparedStatements[sql]; present {
		return c.sendPreparedQuery(ps, arguments...)
	} else {
		return c.sendSimpleQuery(sql, arguments...)
	}
}

func (c *Connection) sendSimpleQuery(sql string, arguments ...interface{}) (err error) {
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

	return c.txMsg('Q', buf, true)
}

func (c *Connection) sendPreparedQuery(ps *preparedStatement, arguments ...interface{}) (err error) {
	if len(ps.ParameterOids) != len(arguments) {
		return fmt.Errorf("Prepared statement \"%v\" requires %d parameters, but %d were provided", ps.Name, len(ps.ParameterOids), len(arguments))
	}

	// bind
	buf := c.getBuf()
	w := newMessageWriter(buf)
	w.WriteCString("")
	w.WriteCString(ps.Name)
	w.Write(int16(len(ps.ParameterOids)))
	for _, oid := range ps.ParameterOids {
		transcoder := ValueTranscoders[oid]
		if transcoder == nil {
			transcoder = defaultTranscoder
		}
		w.Write(transcoder.EncodeFormat)
	}

	w.Write(int16(len(arguments)))
	for i, oid := range ps.ParameterOids {
		if arguments[i] != nil {
			transcoder := ValueTranscoders[oid]
			if transcoder == nil {
				transcoder = defaultTranscoder
			}
			transcoder.EncodeTo(w, arguments[i])
		} else {
			w.Write(int32(-1))
		}
	}

	w.Write(int16(len(ps.FieldDescriptions)))
	for _, fd := range ps.FieldDescriptions {
		transcoder := ValueTranscoders[fd.DataType]
		if transcoder != nil && transcoder.DecodeBinary != nil {
			w.Write(int16(1))
		} else {
			w.Write(int16(0))
		}
	}
	if w.Err != nil {
		return w.Err
	}

	err = c.txMsg('B', buf, false)
	if err != nil {
		return err
	}

	// execute
	buf = c.getBuf()
	w = newMessageWriter(buf)
	w.WriteCString("")
	w.Write(int32(0))

	if w.Err != nil {
		return w.Err
	}

	err = c.txMsg('E', buf, false)
	if err != nil {
		return err
	}

	// sync
	err = c.txMsg('S', c.getBuf(), true)
	if err != nil {
		return err
	}

	return

}

// Execute executes sql. sql can be either a prepared statement name or an SQL string.
// arguments will be sanitized before being interpolated into sql strings. arguments
// should be referenced positionally from the sql string as $1, $2, etc.
func (c *Connection) Execute(sql string, arguments ...interface{}) (commandTag string, err error) {
	defer func() {
		if err != nil {
			c.logger.Error(fmt.Sprintf("Execute `%s` with %v failed: %v", sql, arguments, err))
		}
	}()

	if err = c.sendQuery(sql, arguments...); err != nil {
		return
	}

	for {
		if t, r, rxErr := c.rxMsg(); rxErr == nil {
			switch t {
			case readyForQuery:
				c.rxReadyForQuery(r)
				return
			case rowDescription:
			case dataRow:
			case bindComplete:
			case commandComplete:
				commandTag = r.ReadCString()
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
					err = e
				}
			}
		} else {
			return "", rxErr
		}
	}
}

// Transaction runs f in a transaction. f should return true if the transaction
// should be committed or false if it should be rolled back. Return value committed
// is if the transaction was committed or not. committed should be checked separately
// from err as an explicit rollback is not an error. Transaction will use the default
// isolation level for the current connection. To use a specific isolation level see
// TransactionIso
func (c *Connection) Transaction(f func() bool) (committed bool, err error) {
	return c.transaction("", f)
}

// TransactionIso is the same as Transaction except it takes an isoLevel argument that
// it uses as the transaction isolation level.
//
// Valid isolation levels are:
//   serializable
//   repeatable read
//   read committed
//   read uncommitted
func (c *Connection) TransactionIso(isoLevel string, f func() bool) (committed bool, err error) {
	return c.transaction(isoLevel, f)
}

func (c *Connection) transaction(isoLevel string, f func() bool) (committed bool, err error) {
	var beginSql string
	if isoLevel == "" {
		beginSql = "begin"
	} else {
		beginSql = fmt.Sprintf("begin isolation level %s", isoLevel)
	}

	if _, err = c.Execute(beginSql); err != nil {
		return
	}
	defer func() {
		if committed && c.TxStatus == 'T' {
			_, err = c.Execute("commit")
			if err != nil {
				committed = false
			}
		} else {
			_, err = c.Execute("rollback")
			committed = false
		}
	}()

	committed = f()
	return
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Connection) processContextFreeMsg(t byte, r *MessageReader) (err error) {
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

func (c *Connection) rxMsg() (t byte, r *MessageReader, err error) {
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return
	}

	var body *bytes.Buffer
	if body, err = c.rxMsgBody(bodySize); err != nil {
		return
	}

	r = newMessageReader(body)
	return
}

func (c *Connection) rxMsgHeader() (t byte, bodySize int32, err error) {
	if !c.alive {
		err = errors.New("Connection is dead")
		return
	}

	defer func() {
		if err != nil {
			c.die(err)
		}
	}()

	t, err = c.reader.ReadByte()
	if err != nil {
		return
	}
	err = binary.Read(c.reader, binary.BigEndian, &bodySize)
	bodySize -= 4 // remove self from size
	return
}

func (c *Connection) rxMsgBody(bodySize int32) (*bytes.Buffer, error) {
	if !c.alive {
		return nil, errors.New("Connection is dead")
	}

	buf := c.getBuf()
	_, err := io.CopyN(buf, c.reader, int64(bodySize))
	if err != nil {
		c.die(err)
		return nil, err
	}

	return buf, nil
}

func (c *Connection) rxAuthenticationX(r *MessageReader) (err error) {
	code := r.ReadInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		err = c.txPasswordMessage(c.parameters.Password)
	case 5: // AuthenticationMD5Password
		salt := r.ReadString(4)
		digestedPassword := "md5" + hexMD5(hexMD5(c.parameters.Password+c.parameters.User)+salt)
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

func (c *Connection) rxParameterStatus(r *MessageReader) {
	key := r.ReadCString()
	value := r.ReadCString()
	c.RuntimeParams[key] = value
}

func (c *Connection) rxErrorResponse(r *MessageReader) (err PgError) {
	for {
		switch r.ReadByte() {
		case 'S':
			err.Severity = r.ReadCString()
		case 'C':
			err.Code = r.ReadCString()
		case 'M':
			err.Message = r.ReadCString()
		case 0: // End of error message
			return
		default: // Ignore other error fields
			r.ReadCString()
		}
	}
}

func (c *Connection) rxBackendKeyData(r *MessageReader) {
	c.Pid = r.ReadInt32()
	c.SecretKey = r.ReadInt32()
}

func (c *Connection) rxReadyForQuery(r *MessageReader) {
	c.TxStatus = r.ReadByte()
}

func (c *Connection) rxRowDescription(r *MessageReader) (fields []FieldDescription) {
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

func (c *Connection) rxParameterDescription(r *MessageReader) (parameters []Oid) {
	parameterCount := r.ReadInt16()
	parameters = make([]Oid, 0, parameterCount)
	for i := int16(0); i < parameterCount; i++ {
		parameters = append(parameters, r.ReadOid())
	}
	return
}

func (c *Connection) rxDataRow(r *DataRowReader) (row map[string]interface{}) {
	fieldCount := len(r.fields)

	row = make(map[string]interface{}, fieldCount)
	for i := 0; i < fieldCount; i++ {
		row[r.fields[i].Name] = r.ReadValue()
	}
	return
}

func (c *Connection) rxCommandComplete(r *MessageReader) string {
	return r.ReadCString()
}

func (c *Connection) rxNotificationResponse(r *MessageReader) (err error) {
	n := new(Notification)
	n.Pid = r.ReadInt32()
	n.Channel = r.ReadCString()
	n.Payload = r.ReadCString()
	c.notifications = append(c.notifications, n)
	return
}

func (c *Connection) startTLS() (err error) {
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

	c.conn = tls.Client(c.conn, c.parameters.TLSConfig)

	return nil
}

func (c *Connection) txStartupMessage(msg *startupMessage) (err error) {
	_, err = c.writer.Write(msg.Bytes())
	if err != nil {
		return
	}
	err = c.writer.Flush()
	return
}

func (c *Connection) txMsg(identifier byte, buf *bytes.Buffer, flush bool) (err error) {
	if !c.alive {
		return errors.New("Connection is dead")
	}

	defer func() {
		if err != nil {
			c.die(err)
		}
	}()

	err = binary.Write(c.writer, binary.BigEndian, identifier)
	if err != nil {
		return
	}

	err = binary.Write(c.writer, binary.BigEndian, int32(buf.Len()+4))
	if err != nil {
		return
	}

	_, err = buf.WriteTo(c.writer)
	if err != nil {
		return
	}

	if flush {
		err = c.writer.Flush()
	}

	return
}

func (c *Connection) txPasswordMessage(password string) (err error) {
	buf := c.getBuf()

	_, err = buf.WriteString(password)
	if err != nil {
		return
	}
	buf.WriteByte(0)
	if err != nil {
		return
	}
	err = c.txMsg('p', buf, true)
	return
}

// Gets the shared connection buffer. Since bytes.Buffer never releases memory from
// its internal byte array, check on the size and create a new bytes.Buffer so the
// old one can get GC'ed
func (c *Connection) getBuf() *bytes.Buffer {
	c.buf.Reset()
	if cap(c.buf.Bytes()) > c.bufSize {
		c.logger.Debug(fmt.Sprintf("c.buf (%d) is larger than c.bufSize (%d) -- resetting", cap(c.buf.Bytes()), c.bufSize))
		c.buf = bytes.NewBuffer(make([]byte, 0, c.bufSize))
	}
	return c.buf
}

func (c *Connection) die(err error) {
	c.alive = false
	c.causeOfDeath = err
}
