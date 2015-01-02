package pgx

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	Host      string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port      uint16 // default: 5432
	Database  string
	User      string // default: OS user name
	Password  string
	TLSConfig *tls.Config // config for TLS connection -- nil disables TLS
	Logger    Logger
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	conn               net.Conn      // the underlying TCP or unix domain socket connection
	lastActivityTime   time.Time     // the last time the connection was used
	reader             *bufio.Reader // buffered reader to improve read performance
	wbuf               [1024]byte
	Pid                int32             // backend pid
	SecretKey          int32             // key to use to send a cancel query message to the server
	RuntimeParams      map[string]string // parameters that have been reported by the server
	PgTypes            map[Oid]PgType    // oids to PgTypes
	config             ConnConfig        // config used when establishing this connection
	TxStatus           byte
	preparedStatements map[string]*PreparedStatement
	notifications      []*Notification
	alive              bool
	causeOfDeath       error
	logger             Logger
	mr                 msgReader
	fp                 *fastpath
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

type PgType struct {
	Name          string // name of type e.g. int4, text, date
	DefaultFormat int16  // default format (text or binary) this type will be requested in
}

type CommandTag string

// RowsAffected returns the number of rows affected. If the CommandTag was not
// for a row affecting command (such as "CREATE TABLE") then it returns 0
func (ct CommandTag) RowsAffected() int64 {
	s := string(ct)
	index := strings.LastIndex(s, " ")
	if index == -1 {
		return 0
	}
	n, _ := strconv.ParseInt(s[index+1:], 10, 64)
	return n
}

var ErrNoRows = errors.New("no rows in result set")
var ErrNotificationTimeout = errors.New("notification timeout")
var ErrDeadConn = errors.New("conn is dead")

type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}

// Connect establishes a connection with a PostgreSQL server using config.
// config.Host must be specified. config.User will default to the OS user name.
// Other config fields are optional.
func Connect(config ConnConfig) (c *Conn, err error) {
	c = new(Conn)

	c.config = config
	if c.config.Logger != nil {
		c.logger = c.config.Logger
	} else {
		c.logger = dlogger
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
		d := net.Dialer{KeepAlive: 5 * time.Minute}
		c.conn, err = d.Dial("tcp", fmt.Sprintf("%s:%d", c.config.Host, c.config.Port))
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

	c.RuntimeParams = make(map[string]string)
	c.preparedStatements = make(map[string]*PreparedStatement)
	c.alive = true
	c.lastActivityTime = time.Now()

	if config.TLSConfig != nil {
		c.logger.Debug("Starting TLS handshake")
		if err = c.startTLS(); err != nil {
			c.logger.Error(fmt.Sprintf("TLS failed: %v", err))
			return
		}
	}

	c.reader = bufio.NewReader(c.conn)
	c.mr.reader = c.reader

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
		var r *msgReader
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
			if c.logger != dlogger {
				c.logger = &connLogger{logger: c.logger, pid: c.Pid}
				c.logger.Info("Connection established")
			}

			err = c.loadPgTypes()
			if err != nil {
				return nil, err
			}

			return c, nil
		default:
			if err = c.processContextFreeMsg(t, r); err != nil {
				return nil, err
			}
		}
	}
}

func (c *Conn) loadPgTypes() error {
	rows, err := c.Query("select t.oid, t.typname from pg_type t where t.typtype='b'")
	if err != nil {
		return err
	}

	c.PgTypes = make(map[Oid]PgType, 128)

	for rows.Next() {
		var oid Oid
		var t PgType

		rows.Scan(&oid, &t.Name)

		// The zero value is text format so we ignore any types without a default type format
		t.DefaultFormat, _ = DefaultTypeFormats[t.Name]

		c.PgTypes[oid] = t
	}

	return rows.Err()
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close() (err error) {
	if !c.IsAlive() {
		return nil
	}

	wbuf := newWriteBuf(c.wbuf[0:0], 'X')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)

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

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
func (c *Conn) Prepare(name, sql string) (ps *PreparedStatement, err error) {
	defer func() {
		if err != nil {
			c.logger.Error(fmt.Sprintf("Prepare `%s` as `%s` failed: %v", name, sql, err))
		}
	}()

	// parse
	wbuf := newWriteBuf(c.wbuf[0:0], 'P')
	wbuf.WriteCString(name)
	wbuf.WriteCString(sql)
	wbuf.WriteInt16(0)

	// describe
	wbuf.startMsg('D')
	wbuf.WriteByte('S')
	wbuf.WriteCString(name)

	// sync
	wbuf.startMsg('S')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)
	if err != nil {
		return nil, err
	}

	ps = &PreparedStatement{Name: name}

	var softErr error

	for {
		var t byte
		var r *msgReader
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
				t, _ := c.PgTypes[ps.FieldDescriptions[i].DataType]
				ps.FieldDescriptions[i].DataTypeName = t.Name
				ps.FieldDescriptions[i].FormatCode = t.DefaultFormat
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

	// close
	wbuf := newWriteBuf(c.wbuf[0:0], 'C')
	wbuf.WriteByte('S')
	wbuf.WriteCString(name)

	// flush
	wbuf.startMsg('H')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)
	if err != nil {
		return err
	}

	for {
		var t byte
		var r *msgReader
		t, r, err := c.rxMsg()
		if err != nil {
			return err
		}

		switch t {
		case closeComplete:
			return nil
		default:
			err = c.processContextFreeMsg(t, r)
			if err != nil {
				return err
			}
		}
	}
}

// Listen establishes a PostgreSQL listen/notify to channel
func (c *Conn) Listen(channel string) (err error) {
	_, err = c.Exec("listen " + channel)
	return
}

// WaitForNotification waits for a PostgreSQL notification for up to timeout.
// If the timeout occurs it returns pgx.ErrNotificationTimeout
func (c *Conn) WaitForNotification(timeout time.Duration) (*Notification, error) {
	// Return already received notification immediately
	if len(c.notifications) > 0 {
		notification := c.notifications[0]
		c.notifications = c.notifications[1:]
		return notification, nil
	}

	stopTime := time.Now().Add(timeout)

	for {
		now := time.Now()

		if now.After(stopTime) {
			return nil, ErrNotificationTimeout
		}

		// If there has been no activity on this connection for a while send a nop message just to ensure
		// the connection is alive
		nextEnsureAliveTime := c.lastActivityTime.Add(15 * time.Second)
		if nextEnsureAliveTime.Before(now) {
			// If the server can't respond to a nop in 15 seconds, assume it's dead
			err := c.conn.SetReadDeadline(now.Add(15 * time.Second))
			if err != nil {
				return nil, err
			}

			_, err = c.Exec("--;")
			if err != nil {
				return nil, err
			}

			c.lastActivityTime = now
		}

		var deadline time.Time
		if stopTime.Before(nextEnsureAliveTime) {
			deadline = stopTime
		} else {
			deadline = nextEnsureAliveTime
		}

		notification, err := c.waitForNotification(deadline)
		if err != ErrNotificationTimeout {
			return notification, err
		}
	}
}

func (c *Conn) waitForNotification(deadline time.Time) (*Notification, error) {
	var zeroTime time.Time

	for {
		// Use SetReadDeadline to implement the timeout. SetReadDeadline will
		// cause operations to fail with a *net.OpError that has a Timeout()
		// of true. Because the normal pgx rxMsg path considers any error to
		// have potentially corrupted the state of the connection, it dies
		// on any errors. So to avoid timeout errors in rxMsg we set the
		// deadline and peek into the reader. If a timeout error occurs there
		// we don't break the pgx connection. If the Peek returns that data
		// is available then we turn off the read deadline before the rxMsg.
		err := c.conn.SetReadDeadline(deadline)
		if err != nil {
			return nil, err
		}

		// Wait until there is a byte available before continuing onto the normal msg reading path
		_, err = c.reader.Peek(1)
		if err != nil {
			c.conn.SetReadDeadline(zeroTime) // we can only return one error and we already have one -- so ignore possiple error from SetReadDeadline
			if err, ok := err.(*net.OpError); ok && err.Timeout() {
				return nil, ErrNotificationTimeout
			}
			return nil, err
		}

		err = c.conn.SetReadDeadline(zeroTime)
		if err != nil {
			return nil, err
		}

		var t byte
		var r *msgReader
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

func (c *Conn) sendQuery(sql string, arguments ...interface{}) (err error) {
	if ps, present := c.preparedStatements[sql]; present {
		return c.sendPreparedQuery(ps, arguments...)
	} else {
		return c.sendSimpleQuery(sql, arguments...)
	}
}

func (c *Conn) sendSimpleQuery(sql string, args ...interface{}) error {
	if len(args) == 0 {
		wbuf := newWriteBuf(c.wbuf[0:0], 'Q')
		wbuf.WriteCString(sql)
		wbuf.closeMsg()

		_, err := c.conn.Write(wbuf.buf)
		if err != nil {
			c.die(err)
			return err
		}

		return nil
	}

	ps, err := c.Prepare("", sql)
	if err != nil {
		return err
	}

	return c.sendPreparedQuery(ps, args...)
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
	for i, oid := range ps.ParameterOids {
		switch arg := arguments[i].(type) {
		case Encoder:
			wbuf.WriteInt16(arg.FormatCode())
		case string:
			wbuf.WriteInt16(TextFormatCode)
		default:
			switch oid {
			case BoolOid, ByteaOid, Int2Oid, Int4Oid, Int8Oid, Float4Oid, Float8Oid, TimestampTzOid, TimestampTzArrayOid, TimestampArrayOid, BoolArrayOid, Int2ArrayOid, Int4ArrayOid, Int8ArrayOid, Float4ArrayOid, Float8ArrayOid, TextArrayOid, VarcharArrayOid, OidOid:
				wbuf.WriteInt16(BinaryFormatCode)
			default:
				wbuf.WriteInt16(TextFormatCode)
			}
		}
	}

	wbuf.WriteInt16(int16(len(arguments)))
	for i, oid := range ps.ParameterOids {
		if arguments[i] == nil {
			wbuf.WriteInt32(-1)
			continue
		}

		switch arg := arguments[i].(type) {
		case Encoder:
			err = arg.Encode(wbuf, oid)
		case string:
			err = encodeText(wbuf, arguments[i])
		default:
			switch oid {
			case BoolOid:
				err = encodeBool(wbuf, arguments[i])
			case ByteaOid:
				err = encodeBytea(wbuf, arguments[i])
			case Int2Oid:
				err = encodeInt2(wbuf, arguments[i])
			case Int4Oid:
				err = encodeInt4(wbuf, arguments[i])
			case Int8Oid:
				err = encodeInt8(wbuf, arguments[i])
			case Float4Oid:
				err = encodeFloat4(wbuf, arguments[i])
			case Float8Oid:
				err = encodeFloat8(wbuf, arguments[i])
			case TextOid, VarcharOid:
				err = encodeText(wbuf, arguments[i])
			case DateOid:
				err = encodeDate(wbuf, arguments[i])
			case TimestampTzOid:
				err = encodeTimestampTz(wbuf, arguments[i])
			case TimestampOid:
				err = encodeTimestamp(wbuf, arguments[i])
			case BoolArrayOid:
				err = encodeBoolArray(wbuf, arguments[i])
			case Int2ArrayOid:
				err = encodeInt2Array(wbuf, arguments[i])
			case Int4ArrayOid:
				err = encodeInt4Array(wbuf, arguments[i])
			case Int8ArrayOid:
				err = encodeInt8Array(wbuf, arguments[i])
			case Float4ArrayOid:
				err = encodeFloat4Array(wbuf, arguments[i])
			case Float8ArrayOid:
				err = encodeFloat8Array(wbuf, arguments[i])
			case TextArrayOid:
				err = encodeTextArray(wbuf, arguments[i], TextOid)
			case VarcharArrayOid:
				err = encodeTextArray(wbuf, arguments[i], VarcharOid)
			case TimestampArrayOid:
				err = encodeTimestampArray(wbuf, arguments[i], TimestampOid)
			case TimestampTzArrayOid:
				err = encodeTimestampArray(wbuf, arguments[i], TimestampTzOid)
			case OidOid:
				err = encodeOid(wbuf, arguments[i])
			default:
				return SerializationError(fmt.Sprintf("Cannot encode %T into oid %v - %T must implement Encoder or be converted to a string", arg, oid, arg))
			}
		}
		if err != nil {
			return err
		}
	}

	wbuf.WriteInt16(int16(len(ps.FieldDescriptions)))
	for _, fd := range ps.FieldDescriptions {
		wbuf.WriteInt16(fd.FormatCode)
	}

	// execute
	wbuf.startMsg('E')
	wbuf.WriteByte(0)
	wbuf.WriteInt32(0)

	// sync
	wbuf.startMsg('S')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)
	if err != nil {
		c.die(err)
	}

	return err
}

// Exec executes sql. sql can be either a prepared statement name or an SQL string.
// arguments should be referenced positionally from the sql string as $1, $2, etc.
func (c *Conn) Exec(sql string, arguments ...interface{}) (commandTag CommandTag, err error) {
	startTime := time.Now()
	c.lastActivityTime = startTime

	if c.logger != dlogger {
		defer func() {
			if err == nil {
				endTime := time.Now()
				c.logger.Info("Exec", "sql", sql, "args", logQueryArgs(arguments), "time", endTime.Sub(startTime), "commandTag", commandTag)
			} else {
				c.logger.Error("Exec", "sql", sql, "args", logQueryArgs(arguments), "error", err)
			}
		}()
	}

	if err = c.sendQuery(sql, arguments...); err != nil {
		return
	}

	var softErr error

	for {
		var t byte
		var r *msgReader
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
			commandTag = CommandTag(r.readCString())
		default:
			if e := c.processContextFreeMsg(t, r); e != nil && softErr == nil {
				softErr = e
			}
		}
	}
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Conn) processContextFreeMsg(t byte, r *msgReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	case noticeResponse:
		return nil
	case emptyQueryResponse:
		return nil
	case notificationResponse:
		c.rxNotificationResponse(r)
		return nil
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}
}

func (c *Conn) rxMsg() (t byte, r *msgReader, err error) {
	if !c.alive {
		return 0, nil, ErrDeadConn
	}

	t, err = c.mr.rxMsg()
	if err != nil {
		c.die(err)
	}

	c.lastActivityTime = time.Now()

	return t, &c.mr, err
}

func (c *Conn) rxAuthenticationX(r *msgReader) (err error) {
	switch r.readInt32() {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		err = c.txPasswordMessage(c.config.Password)
	case 5: // AuthenticationMD5Password
		salt := r.readString(4)
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

func (c *Conn) rxParameterStatus(r *msgReader) {
	key := r.readCString()
	value := r.readCString()
	c.RuntimeParams[key] = value
}

func (c *Conn) rxErrorResponse(r *msgReader) (err PgError) {
	for {
		switch r.readByte() {
		case 'S':
			err.Severity = r.readCString()
		case 'C':
			err.Code = r.readCString()
		case 'M':
			err.Message = r.readCString()
		case 'D':
			err.Detail = r.readCString()
		case 'H':
			err.Hint = r.readCString()
		case 's':
			err.SchemaName = r.readCString()
		case 't':
			err.TableName = r.readCString()
		case 'c':
			err.ColumnName = r.readCString()
		case 'd':
			err.DataTypeName = r.readCString()
		case 'n':
			err.ConstraintName = r.readCString()
		case 0: // End of error message
			if err.Severity == "FATAL" {
				c.die(err)
			}
			return
		default: // Ignore other error fields
			r.readCString()
		}
	}
}

func (c *Conn) rxBackendKeyData(r *msgReader) {
	c.Pid = r.readInt32()
	c.SecretKey = r.readInt32()
}

func (c *Conn) rxReadyForQuery(r *msgReader) {
	c.TxStatus = r.readByte()
}

func (c *Conn) rxRowDescription(r *msgReader) (fields []FieldDescription) {
	fieldCount := r.readInt16()
	fields = make([]FieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.Name = r.readCString()
		f.Table = r.readOid()
		f.AttributeNumber = r.readInt16()
		f.DataType = r.readOid()
		f.DataTypeSize = r.readInt16()
		f.Modifier = r.readInt32()
		f.FormatCode = r.readInt16()
	}
	return
}

func (c *Conn) rxParameterDescription(r *msgReader) (parameters []Oid) {
	parameterCount := r.readInt16()
	parameters = make([]Oid, 0, parameterCount)

	for i := int16(0); i < parameterCount; i++ {
		parameters = append(parameters, r.readOid())
	}
	return
}

func (c *Conn) rxNotificationResponse(r *msgReader) {
	n := new(Notification)
	n.Pid = r.readInt32()
	n.Channel = r.readCString()
	n.Payload = r.readCString()
	c.notifications = append(c.notifications, n)
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

func (c *Conn) txPasswordMessage(password string) (err error) {
	wbuf := newWriteBuf(c.wbuf[0:0], 'p')
	wbuf.WriteCString(password)
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)

	return err
}

func (c *Conn) die(err error) {
	c.alive = false
	c.causeOfDeath = err
	c.conn.Close()
}
