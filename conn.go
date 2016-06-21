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
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DialFunc is a function that can be used to connect to a PostgreSQL server
type DialFunc func(network, addr string) (net.Conn, error)

// ConnConfig contains all the options used to establish a connection.
type ConnConfig struct {
	Host              string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port              uint16 // default: 5432
	Database          string
	User              string // default: OS user name
	Password          string
	TLSConfig         *tls.Config // config for TLS connection -- nil disables TLS
	UseFallbackTLS    bool        // Try FallbackTLSConfig if connecting with TLSConfig fails. Used for preferring TLS, but allowing unencrypted, or vice-versa
	FallbackTLSConfig *tls.Config // config for fallback TLS connection (only used if UseFallBackTLS is true)-- nil disables TLS
	Logger            Logger
	LogLevel          int
	Dial              DialFunc
	RuntimeParams     map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)
}

// Conn is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnPool to manage access to multiple database connections from multiple
// goroutines.
type Conn struct {
	conn               net.Conn      // the underlying TCP or unix domain socket connection
	lastActivityTime   time.Time     // the last time the connection was used
	reader             *bufio.Reader // buffered reader to improve read performance
	wbuf               [1024]byte
	writeBuf           WriteBuf
	Pid                int32             // backend pid
	SecretKey          int32             // key to use to send a cancel query message to the server
	RuntimeParams      map[string]string // parameters that have been reported by the server
	PgTypes            map[Oid]PgType    // oids to PgTypes
	config             ConnConfig        // config used when establishing this connection
	TxStatus           byte
	preparedStatements map[string]*PreparedStatement
	channels           map[string]struct{}
	notifications      []*Notification
	alive              bool
	causeOfDeath       error
	logger             Logger
	logLevel           int
	mr                 msgReader
	fp                 *fastpath
	pgsql_af_inet      *byte
	pgsql_af_inet6     *byte
	busy               bool
	poolResetCount     int
	preallocatedRows   []Rows
}

// PreparedStatement is a description of a prepared statement
type PreparedStatement struct {
	Name              string
	SQL               string
	FieldDescriptions []FieldDescription
	ParameterOids     []Oid
}

// PrepareExOptions is an option struct that can be passed to PrepareEx
type PrepareExOptions struct {
	ParameterOids []Oid
}

// Notification is a message received from the PostgreSQL LISTEN/NOTIFY system
type Notification struct {
	Pid     int32  // backend pid that sent the notification
	Channel string // channel from which notification was received
	Payload string
}

// PgType is information about PostgreSQL type and how to encode and decode it
type PgType struct {
	Name          string // name of type e.g. int4, text, date
	DefaultFormat int16  // default format (text or binary) this type will be requested in
}

// CommandTag is the result of an Exec function
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

// ErrNoRows occurs when rows are expected but none are returned.
var ErrNoRows = errors.New("no rows in result set")

// ErrNotificationTimeout occurs when WaitForNotification times out.
var ErrNotificationTimeout = errors.New("notification timeout")

// ErrDeadConn occurs on an attempt to use a dead connection
var ErrDeadConn = errors.New("conn is dead")

// ErrTLSRefused occurs when the connection attempt requires TLS and the
// PostgreSQL server refuses to use TLS
var ErrTLSRefused = errors.New("server refused TLS connection")

// ErrConnBusy occurs when the connection is busy (for example, in the middle of
// reading query results) and another action is attempts.
var ErrConnBusy = errors.New("conn is busy")

// ErrInvalidLogLevel occurs on attempt to set an invalid log level.
var ErrInvalidLogLevel = errors.New("invalid log level")

// ProtocolError occurs when unexpected data is received from PostgreSQL
type ProtocolError string

func (e ProtocolError) Error() string {
	return string(e)
}

// Connect establishes a connection with a PostgreSQL server using config.
// config.Host must be specified. config.User will default to the OS user name.
// Other config fields are optional.
func Connect(config ConnConfig) (c *Conn, err error) {
	return connect(config, nil, nil, nil)
}

func connect(config ConnConfig, pgTypes map[Oid]PgType, pgsql_af_inet *byte, pgsql_af_inet6 *byte) (c *Conn, err error) {
	c = new(Conn)

	c.config = config

	if pgTypes != nil {
		c.PgTypes = make(map[Oid]PgType, len(pgTypes))
		for k, v := range pgTypes {
			c.PgTypes[k] = v
		}
	}

	if pgsql_af_inet != nil {
		c.pgsql_af_inet = new(byte)
		*c.pgsql_af_inet = *pgsql_af_inet
	}
	if pgsql_af_inet6 != nil {
		c.pgsql_af_inet6 = new(byte)
		*c.pgsql_af_inet6 = *pgsql_af_inet6
	}

	if c.config.LogLevel != 0 {
		c.logLevel = c.config.LogLevel
	} else {
		// Preserve pre-LogLevel behavior by defaulting to LogLevelDebug
		c.logLevel = LogLevelDebug
	}
	c.logger = c.config.Logger
	c.mr.log = c.log
	c.mr.shouldLog = c.shouldLog

	if c.config.User == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		c.config.User = user.Username
		if c.shouldLog(LogLevelDebug) {
			c.log(LogLevelDebug, "Using default connection config", "User", c.config.User)
		}
	}

	if c.config.Port == 0 {
		c.config.Port = 5432
		if c.shouldLog(LogLevelDebug) {
			c.log(LogLevelDebug, "Using default connection config", "Port", c.config.Port)
		}
	}

	network := "tcp"
	address := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
	// See if host is a valid path, if yes connect with a socket
	if _, err := os.Stat(c.config.Host); err == nil {
		// For backward compatibility accept socket file paths -- but directories are now preferred
		network = "unix"
		address = c.config.Host
		if !strings.Contains(address, "/.s.PGSQL.") {
			address = filepath.Join(address, ".s.PGSQL.") + strconv.FormatInt(int64(c.config.Port), 10)
		}
	}
	if c.config.Dial == nil {
		c.config.Dial = (&net.Dialer{KeepAlive: 5 * time.Minute}).Dial
	}

	err = c.connect(config, network, address, config.TLSConfig)
	if err != nil && config.UseFallbackTLS {
		err = c.connect(config, network, address, config.FallbackTLSConfig)
	}

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Conn) connect(config ConnConfig, network, address string, tlsConfig *tls.Config) (err error) {
	if c.shouldLog(LogLevelInfo) {
		c.log(LogLevelInfo, fmt.Sprintf("Dialing PostgreSQL server at %s address: %s", network, address))
	}
	c.conn, err = c.config.Dial(network, address)
	if err != nil {
		if c.shouldLog(LogLevelError) {
			c.log(LogLevelError, fmt.Sprintf("Connection failed: %v", err))
		}
		return err
	}
	defer func() {
		if c != nil && err != nil {
			c.conn.Close()
			c.alive = false
			if c.shouldLog(LogLevelError) {
				c.log(LogLevelError, err.Error())
			}
		}
	}()

	c.RuntimeParams = make(map[string]string)
	c.preparedStatements = make(map[string]*PreparedStatement)
	c.channels = make(map[string]struct{})
	c.alive = true
	c.lastActivityTime = time.Now()

	if tlsConfig != nil {
		if c.shouldLog(LogLevelDebug) {
			c.log(LogLevelDebug, "Starting TLS handshake")
		}
		if err := c.startTLS(tlsConfig); err != nil {
			if c.shouldLog(LogLevelError) {
				c.log(LogLevelError, fmt.Sprintf("TLS failed: %v", err))
			}
			return err
		}
	}

	c.reader = bufio.NewReader(c.conn)
	c.mr.reader = c.reader

	msg := newStartupMessage()

	// Default to disabling TLS renegotiation.
	//
	// Go does not support (https://github.com/golang/go/issues/5742)
	// PostgreSQL recommends disabling (http://www.postgresql.org/docs/9.4/static/runtime-config-connection.html#GUC-SSL-RENEGOTIATION-LIMIT)
	if tlsConfig != nil {
		msg.options["ssl_renegotiation_limit"] = "0"
	}

	// Copy default run-time params
	for k, v := range config.RuntimeParams {
		msg.options[k] = v
	}

	msg.options["user"] = c.config.User
	if c.config.Database != "" {
		msg.options["database"] = c.config.Database
	}

	if err = c.txStartupMessage(msg); err != nil {
		return err
	}

	for {
		var t byte
		var r *msgReader
		t, r, err = c.rxMsg()
		if err != nil {
			return err
		}

		switch t {
		case backendKeyData:
			c.rxBackendKeyData(r)
		case authenticationX:
			if err = c.rxAuthenticationX(r); err != nil {
				return err
			}
		case readyForQuery:
			c.rxReadyForQuery(r)
			if c.shouldLog(LogLevelInfo) {
				c.log(LogLevelInfo, "Connection established")
			}

			if c.PgTypes == nil {
				err = c.loadPgTypes()
				if err != nil {
					return err
				}
			}

			if c.pgsql_af_inet == nil || c.pgsql_af_inet6 == nil {
				err = c.loadInetConstants()
				if err != nil {
					return err
				}
			}

			return nil
		default:
			if err = c.processContextFreeMsg(t, r); err != nil {
				return err
			}
		}
	}
}

func (c *Conn) loadPgTypes() error {
	rows, err := c.Query(`select t.oid, t.typname
from pg_type t
left join pg_type base_type on t.typelem=base_type.oid
where (
	  t.typtype='b'
	  and (base_type.oid is null or base_type.typtype='b')
	)
  or t.typname in('record');`)
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

// Family is needed for binary encoding of inet/cidr. The constant is based on
// the server's definition of AF_INET. In theory, this could differ between
// platforms, so request an IPv4 and an IPv6 inet and get the family from that.
func (c *Conn) loadInetConstants() error {
	var ipv4, ipv6 []byte

	err := c.QueryRow("select '127.0.0.1'::inet, '1::'::inet").Scan(&ipv4, &ipv6)
	if err != nil {
		return err
	}

	c.pgsql_af_inet = &ipv4[0]
	c.pgsql_af_inet6 = &ipv6[0]

	return nil
}

// Close closes a connection. It is safe to call Close on a already closed
// connection.
func (c *Conn) Close() (err error) {
	if !c.IsAlive() {
		return nil
	}

	wbuf := newWriteBuf(c, 'X')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)

	c.die(errors.New("Closed"))
	if c.shouldLog(LogLevelInfo) {
		c.log(LogLevelInfo, "Closed connection")
	}
	return err
}

// ParseURI parses a database URI into ConnConfig
//
// Query parameters not used by the connection process are parsed into ConnConfig.RuntimeParams.
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

	err = configSSL(url.Query().Get("sslmode"), &cp)
	if err != nil {
		return cp, err
	}

	ignoreKeys := map[string]struct{}{
		"sslmode": struct{}{},
	}

	cp.RuntimeParams = make(map[string]string)

	for k, v := range url.Query() {
		if _, ok := ignoreKeys[k]; ok {
			continue
		}

		cp.RuntimeParams[k] = v[0]
	}

	return cp, nil
}

var dsn_regexp = regexp.MustCompile(`([a-zA-Z_]+)=((?:"[^"]+")|(?:[^ ]+))`)

// ParseDSN parses a database DSN (data source name) into a ConnConfig
//
// e.g. ParseDSN("user=username password=password host=1.2.3.4 port=5432 dbname=mydb sslmode=disable")
//
// Any options not used by the connection process are parsed into ConnConfig.RuntimeParams.
//
// e.g. ParseDSN("application_name=pgxtest search_path=admin user=username password=password host=1.2.3.4 dbname=mydb")
//
// ParseDSN tries to match libpq behavior with regard to sslmode. See comments
// for ParseEnvLibpq for more information on the security implications of
// sslmode options.
func ParseDSN(s string) (ConnConfig, error) {
	var cp ConnConfig

	m := dsn_regexp.FindAllStringSubmatch(s, -1)

	var sslmode string

	cp.RuntimeParams = make(map[string]string)

	for _, b := range m {
		switch b[1] {
		case "user":
			cp.User = b[2]
		case "password":
			cp.Password = b[2]
		case "host":
			cp.Host = b[2]
		case "port":
			if p, err := strconv.ParseUint(b[2], 10, 16); err != nil {
				return cp, err
			} else {
				cp.Port = uint16(p)
			}
		case "dbname":
			cp.Database = b[2]
		case "sslmode":
			sslmode = b[2]
		default:
			cp.RuntimeParams[b[1]] = b[2]
		}
	}

	err := configSSL(sslmode, &cp)
	if err != nil {
		return cp, err
	}

	return cp, nil
}

// ParseEnvLibpq parses the environment like libpq does into a ConnConfig
//
// See http://www.postgresql.org/docs/9.4/static/libpq-envars.html for details
// on the meaning of environment variables.
//
// ParseEnvLibpq currently recognizes the following environment variables:
// PGHOST
// PGPORT
// PGDATABASE
// PGUSER
// PGPASSWORD
// PGSSLMODE
// PGAPPNAME
//
// Important TLS Security Notes:
// ParseEnvLibpq tries to match libpq behavior with regard to PGSSLMODE. This
// includes defaulting to "prefer" behavior if no environment variable is set.
//
// See http://www.postgresql.org/docs/9.4/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION
// for details on what level of security each sslmode provides.
//
// "require" and "verify-ca" modes currently are treated as "verify-full". e.g.
// They have stronger security guarantees than they would with libpq. Do not
// rely on this behavior as it may be possible to match libpq in the future. If
// you need full security use "verify-full".
//
// Several of the PGSSLMODE options (including the default behavior of "prefer")
// will set UseFallbackTLS to true and FallbackTLSConfig to a disabled or
// weakened TLS mode. This means that if ParseEnvLibpq is used, but TLSConfig is
// later set from a different source that UseFallbackTLS MUST be set false to
// avoid the possibility of falling back to weaker or disabled security.
func ParseEnvLibpq() (ConnConfig, error) {
	var cc ConnConfig

	cc.Host = os.Getenv("PGHOST")

	if pgport := os.Getenv("PGPORT"); pgport != "" {
		if port, err := strconv.ParseUint(pgport, 10, 16); err == nil {
			cc.Port = uint16(port)
		} else {
			return cc, err
		}
	}

	cc.Database = os.Getenv("PGDATABASE")
	cc.User = os.Getenv("PGUSER")
	cc.Password = os.Getenv("PGPASSWORD")

	sslmode := os.Getenv("PGSSLMODE")

	err := configSSL(sslmode, &cc)
	if err != nil {
		return cc, err
	}

	cc.RuntimeParams = make(map[string]string)
	if appname := os.Getenv("PGAPPNAME"); appname != "" {
		cc.RuntimeParams["application_name"] = appname
	}

	return cc, nil
}

func configSSL(sslmode string, cc *ConnConfig) error {
	// Match libpq default behavior
	if sslmode == "" {
		sslmode = "prefer"
	}

	switch sslmode {
	case "disable":
	case "allow":
		cc.UseFallbackTLS = true
		cc.FallbackTLSConfig = &tls.Config{InsecureSkipVerify: true}
	case "prefer":
		cc.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		cc.UseFallbackTLS = true
		cc.FallbackTLSConfig = nil
	case "require", "verify-ca", "verify-full":
		cc.TLSConfig = &tls.Config{
			ServerName: cc.Host,
		}
	default:
		return errors.New("sslmode is invalid")
	}

	return nil
}

// Prepare creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
//
// Prepare is idempotent; i.e. it is safe to call Prepare multiple times with the same
// name and sql arguments. This allows a code path to Prepare and Query/Exec without
// concern for if the statement has already been prepared.
func (c *Conn) Prepare(name, sql string) (ps *PreparedStatement, err error) {
	return c.PrepareEx(name, sql, nil)
}

// PrepareEx creates a prepared statement with name and sql. sql can contain placeholders
// for bound parameters. These placeholders are referenced positional as $1, $2, etc.
// It defers from Prepare as it allows additional options (such as parameter OIDs) to be passed via struct
//
// PrepareEx is idempotent; i.e. it is safe to call PrepareEx multiple times with the same
// name and sql arguments. This allows a code path to PrepareEx and Query/Exec without
// concern for if the statement has already been prepared.
func (c *Conn) PrepareEx(name, sql string, opts *PrepareExOptions) (ps *PreparedStatement, err error) {
	if name != "" {
		if ps, ok := c.preparedStatements[name]; ok && ps.SQL == sql {
			return ps, nil
		}
	}

	if c.shouldLog(LogLevelError) {
		defer func() {
			if err != nil {
				c.log(LogLevelError, fmt.Sprintf("Prepare `%s` as `%s` failed: %v", name, sql, err))
			}
		}()
	}

	// parse
	wbuf := newWriteBuf(c, 'P')
	wbuf.WriteCString(name)
	wbuf.WriteCString(sql)

	if opts != nil {
		if len(opts.ParameterOids) > 65535 {
			return nil, errors.New(fmt.Sprintf("Number of PrepareExOptions ParameterOids must be between 0 and 65535, received %d", len(opts.ParameterOids)))
		}
		wbuf.WriteInt16(int16(len(opts.ParameterOids)))
		for _, oid := range opts.ParameterOids {
			wbuf.WriteInt32(int32(oid))
		}
	} else {
		wbuf.WriteInt16(0)
	}

	// describe
	wbuf.startMsg('D')
	wbuf.WriteByte('S')
	wbuf.WriteCString(name)

	// sync
	wbuf.startMsg('S')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)
	if err != nil {
		c.die(err)
		return nil, err
	}

	ps = &PreparedStatement{Name: name, SQL: sql}

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

			if len(ps.ParameterOids) > 65535 && softErr == nil {
				softErr = fmt.Errorf("PostgreSQL supports maximum of 65535 parameters, received %d", len(ps.ParameterOids))
			}
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

			if softErr == nil {
				c.preparedStatements[name] = ps
			}

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
	wbuf := newWriteBuf(c, 'C')
	wbuf.WriteByte('S')
	wbuf.WriteCString(name)

	// flush
	wbuf.startMsg('H')
	wbuf.closeMsg()

	_, err = c.conn.Write(wbuf.buf)
	if err != nil {
		c.die(err)
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
func (c *Conn) Listen(channel string) error {
	_, err := c.Exec("listen " + quoteIdentifier(channel))
	if err != nil {
		return err
	}

	c.channels[channel] = struct{}{}

	return nil
}

// Unlisten unsubscribes from a listen channel
func (c *Conn) Unlisten(channel string) error {
	_, err := c.Exec("unlisten " + quoteIdentifier(channel))
	if err != nil {
		return err
	}

	delete(c.channels, channel)
	return nil
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
	}
	return c.sendSimpleQuery(sql, arguments...)
}

func (c *Conn) sendSimpleQuery(sql string, args ...interface{}) error {

	if len(args) == 0 {
		wbuf := newWriteBuf(c, 'Q')
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
	wbuf := newWriteBuf(c, 'B')
	wbuf.WriteByte(0)
	wbuf.WriteCString(ps.Name)

	wbuf.WriteInt16(int16(len(ps.ParameterOids)))
	for i, oid := range ps.ParameterOids {
		switch arg := arguments[i].(type) {
		case Encoder:
			wbuf.WriteInt16(arg.FormatCode())
		case string, *string:
			wbuf.WriteInt16(TextFormatCode)
		default:
			switch oid {
			case BoolOid, ByteaOid, Int2Oid, Int4Oid, Int8Oid, Float4Oid, Float8Oid, TimestampTzOid, TimestampTzArrayOid, TimestampOid, TimestampArrayOid, DateOid, BoolArrayOid, ByteaArrayOid, Int2ArrayOid, Int4ArrayOid, Int8ArrayOid, Float4ArrayOid, Float8ArrayOid, TextArrayOid, VarcharArrayOid, OidOid, InetOid, CidrOid, InetArrayOid, CidrArrayOid, RecordOid:
				wbuf.WriteInt16(BinaryFormatCode)
			default:
				wbuf.WriteInt16(TextFormatCode)
			}
		}
	}

	wbuf.WriteInt16(int16(len(arguments)))
	for i, oid := range ps.ParameterOids {
		if err := Encode(wbuf, oid, arguments[i]); err != nil {
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
	if err = c.lock(); err != nil {
		return commandTag, err
	}

	startTime := time.Now()
	c.lastActivityTime = startTime

	defer func() {
		if err == nil {
			if c.shouldLog(LogLevelInfo) {
				endTime := time.Now()
				c.log(LogLevelInfo, "Exec", "sql", sql, "args", logQueryArgs(arguments), "time", endTime.Sub(startTime), "commandTag", commandTag)
			}
		} else {
			if c.shouldLog(LogLevelError) {
				c.log(LogLevelError, "Exec", "sql", sql, "args", logQueryArgs(arguments), "error", err)
			}
		}

		if unlockErr := c.unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

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

	if c.shouldLog(LogLevelTrace) {
		c.log(LogLevelTrace, "rxMsg", "type", string(t), "msgBytesRemaining", c.mr.msgBytesRemaining)
	}

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
		case 'P':
			s := r.readCString()
			n, _ := strconv.ParseInt(s, 10, 32)
			err.Position = int32(n)
		case 'p':
			s := r.readCString()
			n, _ := strconv.ParseInt(s, 10, 32)
			err.InternalPosition = int32(n)
		case 'q':
			err.InternalQuery = r.readCString()
		case 'W':
			err.Where = r.readCString()
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
		case 'F':
			err.File = r.readCString()
		case 'L':
			s := r.readCString()
			n, _ := strconv.ParseInt(s, 10, 32)
			err.Line = int32(n)
		case 'R':
			err.Routine = r.readCString()

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
	// Internally, PostgreSQL supports greater than 64k parameters to a prepared
	// statement. But the parameter description uses a 16-bit integer for the
	// count of parameters. If there are more than 64K parameters, this count is
	// wrong. So read the count, ignore it, and compute the proper value from
	// the size of the message.
	r.readInt16()
	parameterCount := r.msgBytesRemaining / 4

	parameters = make([]Oid, 0, parameterCount)

	for i := int32(0); i < parameterCount; i++ {
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

func (c *Conn) startTLS(tlsConfig *tls.Config) (err error) {
	err = binary.Write(c.conn, binary.BigEndian, []int32{8, 80877103})
	if err != nil {
		return
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(c.conn, response); err != nil {
		return
	}

	if response[0] != 'S' {
		return ErrTLSRefused
	}

	c.conn = tls.Client(c.conn, tlsConfig)

	return nil
}

func (c *Conn) txStartupMessage(msg *startupMessage) error {
	_, err := c.conn.Write(msg.Bytes())
	return err
}

func (c *Conn) txPasswordMessage(password string) (err error) {
	wbuf := newWriteBuf(c, 'p')
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

func (c *Conn) lock() error {
	if c.busy {
		return ErrConnBusy
	}
	c.busy = true
	return nil
}

func (c *Conn) unlock() error {
	if !c.busy {
		return errors.New("unlock conn that is not busy")
	}
	c.busy = false
	return nil
}

func (c *Conn) shouldLog(lvl int) bool {
	return c.logger != nil && c.logLevel >= lvl
}

func (c *Conn) log(lvl int, msg string, ctx ...interface{}) {
	if c.Pid != 0 {
		ctx = append(ctx, "pid", c.Pid)
	}

	switch lvl {
	case LogLevelTrace:
		c.logger.Debug(msg, ctx...)
	case LogLevelDebug:
		c.logger.Debug(msg, ctx...)
	case LogLevelInfo:
		c.logger.Info(msg, ctx...)
	case LogLevelWarn:
		c.logger.Warn(msg, ctx...)
	case LogLevelError:
		c.logger.Error(msg, ctx...)
	}
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
