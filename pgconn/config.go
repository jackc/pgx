package pgconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgpassfile"
	"github.com/jackc/pgservicefile"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
)

type (
	AfterConnectFunc    func(ctx context.Context, pgconn *PgConn) error
	ValidateConnectFunc func(ctx context.Context, pgconn *PgConn) error
	GetSSLPasswordFunc  func(ctx context.Context) string
)

// Config is the settings used to establish a connection to a PostgreSQL server. It must be created by [ParseConfig]. A
// manually initialized Config will cause ConnectConfig to panic.
type Config struct {
	Host           string // host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
	Port           uint16
	Database       string
	User           string
	Password       string
	TLSConfig      *tls.Config // nil disables TLS
	ConnectTimeout time.Duration
	DialFunc       DialFunc   // e.g. net.Dialer.DialContext
	LookupFunc     LookupFunc // e.g. net.Resolver.LookupHost
	BuildFrontend  BuildFrontendFunc

	// MaxProtocolMessageBodyLen is the maximum length of a PostgreSQL wire protocol message body in octets. If a
	// message body exceeds this length, reading the message will fail with pgproto3.ExceededMaxBodyLenErr. The default
	// value is 0, which means no maximum is enforced.
	MaxProtocolMessageBodyLen int

	// BuildContextWatcherHandler is called to create a ContextWatcherHandler for a connection. The handler is called
	// when a context passed to a PgConn method is canceled.
	BuildContextWatcherHandler func(*PgConn) ctxwatch.Handler

	RuntimeParams map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)

	KerberosSrvName string
	KerberosSpn     string
	Fallbacks       []*FallbackConfig

	SSLNegotiation string // sslnegotiation=postgres or sslnegotiation=direct

	// AfterNetConnect is called after the network connection, including TLS if applicable, is established but before any
	// PostgreSQL protocol communication. It takes the established net.Conn and returns a net.Conn that will be used in
	// its place. It can be used to wrap the net.Conn (e.g. for logging, diagnostics, or testing). Its functionality has
	// some overlap with DialFunc. However, DialFunc takes place before TLS is established and cannot be used to control
	// the final net.Conn used for PostgreSQL protocol communication while AfterNetConnect can.
	AfterNetConnect func(ctx context.Context, config *Config, conn net.Conn) (net.Conn, error)

	// ValidateConnect is called during a connection attempt after a successful authentication with the PostgreSQL server.
	// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
	// fallback config is tried. This allows implementing high availability behavior such as libpq does with target_session_attrs.
	ValidateConnect ValidateConnectFunc

	// AfterConnect is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
	// or prepare statements). If this returns an error the connection attempt fails.
	AfterConnect AfterConnectFunc

	// OnNotice is a callback function called when a notice response is received.
	OnNotice NoticeHandler

	// OnNotification is a callback function called when a notification from the LISTEN/NOTIFY system is received.
	OnNotification NotificationHandler

	// OnPgError is a callback function called when a Postgres error is received by the server. The default handler will close
	// the connection on any FATAL errors. If you override this handler you should call the previously set handler or ensure
	// that you close on FATAL errors by returning false.
	OnPgError PgErrorHandler

	// OAuthTokenProvider is a function that returns an OAuth token for authentication. If set, it will be used for
	// OAUTHBEARER SASL authentication when the server requests it.
	OAuthTokenProvider func(context.Context) (string, error)

	// MinProtocolVersion is the minimum acceptable PostgreSQL protocol version.
	// If the server does not support at least this version, the connection will fail.
	// Valid values: "3.0", "3.2", "latest". Defaults to "3.0".
	MinProtocolVersion string

	// MaxProtocolVersion is the maximum PostgreSQL protocol version to request from the server.
	// Valid values: "3.0", "3.2", "latest". Defaults to "3.0" for compatibility.
	MaxProtocolVersion string

	// ChannelBinding is the channel_binding parameter for SCRAM-SHA-256-PLUS authentication.
	// Valid values: "disable", "prefer", "require". Defaults to "prefer".
	ChannelBinding string

	// RequireAuth restricts which authentication methods the client will accept from the server,
	// matching libpq's require_auth parameter. It is a comma-separated list of method names
	// (password, md5, gss, sspi, scram-sha-256, oauth, none). A leading "!" on every entry negates
	// the list (forbid these methods, allow all others). Empty (the default) means all methods are
	// accepted.
	RequireAuth string

	createdByParseConfig bool // Used to enforce created by ParseConfig rule.
}

// defaultPort is the port used when neither the connection string, the environment, nor the
// service file supplies one. It is also the per-element fallback for empty entries in a
// multi-host port list.
const defaultPort = "5432"

// connStringKeyAliases maps libpq parameter keywords to the canonical key names this package
// uses internally in the parsed-settings map. Most keywords are already canonical; this map
// holds only those whose pgx-internal name differs from the libpq spelling.
var connStringKeyAliases = map[string]string{
	"dbname": "database",
}

// canonicalConnStringKey returns the canonical settings-map key for a libpq parameter keyword.
func canonicalConnStringKey(k string) string {
	if c, ok := connStringKeyAliases[k]; ok {
		return c
	}
	return k
}

// ParseConfigOptions contains options that control how a config is built such as GetSSLPassword.
type ParseConfigOptions struct {
	// GetSSLPassword gets the password to decrypt a SSL client certificate. This is analogous to the libpq function
	// PQsetSSLKeyPassHook_OpenSSL.
	GetSSLPassword GetSSLPasswordFunc

	// ConnStringAllowedKeys, if non-nil, restricts which parameter keys may appear in connString
	// itself. Any other key (whether connString is in keyword/value or URL form) causes
	// ParseConfigWithOptions to return an error before any filesystem access or network
	// resolution is attempted. Environment variables (PGHOST, PGSERVICEFILE, ...) and built-in
	// defaults are not checked: only keys that originate from the connString argument.
	//
	// Keys may be given in either their libpq spelling ("dbname") or pgx-internal spelling
	// ("database"); both are accepted. The URI-only ssl=true alias for sslmode=require is
	// accepted when either "ssl" or "sslmode" is allowed; an explicit sslmode key or a
	// non-"true" ssl value only matches its own spelling. Every ssl/sslmode occurrence in
	// the connection string is validated, including occurrences superseded by a later
	// repeated parameter.
	//
	// A nil slice (the default) applies no restriction and matches libpq behaviour. An empty
	// non-nil slice rejects every key, i.e. connString must be empty.
	//
	// Use this when any part of connString is built from input the application does not fully
	// control (tenant configuration, RPC parameters, admin UI fields). List only the keys that
	// input is expected to supply. This fails closed: a future libpq parameter that pgconn learns
	// to parse will be rejected unless the application has explicitly allowed it, rather than
	// silently passing through.
	ConnStringAllowedKeys []string
}

// Copy returns a deep copy of the config that is safe to use and modify.
// The only exception is the TLSConfig field:
// according to the tls.Config docs it must not be modified after creation.
func (c *Config) Copy() *Config {
	newConf := new(Config)
	*newConf = *c
	if newConf.TLSConfig != nil {
		newConf.TLSConfig = c.TLSConfig.Clone()
	}
	if newConf.RuntimeParams != nil {
		newConf.RuntimeParams = make(map[string]string, len(c.RuntimeParams))
		maps.Copy(newConf.RuntimeParams, c.RuntimeParams)
	}
	if newConf.Fallbacks != nil {
		newConf.Fallbacks = make([]*FallbackConfig, len(c.Fallbacks))
		for i, fallback := range c.Fallbacks {
			newFallback := new(FallbackConfig)
			*newFallback = *fallback
			if newFallback.TLSConfig != nil {
				newFallback.TLSConfig = fallback.TLSConfig.Clone()
			}
			newConf.Fallbacks[i] = newFallback
		}
	}
	return newConf
}

// FallbackConfig is additional settings to attempt a connection with when the primary Config fails to establish a
// network connection. It is used for TLS fallback such as sslmode=prefer and high availability (HA) connections.
type FallbackConfig struct {
	Host      string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port      uint16
	TLSConfig *tls.Config // nil disables TLS
}

// connectOneConfig is the configuration for a single attempt to connect to a single host.
type connectOneConfig struct {
	network          string
	address          string
	originalHostname string      // original hostname before resolving
	tlsConfig        *tls.Config // nil disables TLS
}

// isAbsolutePath checks if the provided value is an absolute path either
// beginning with a forward slash (as on Linux-based systems) or with a capital
// letter A-Z followed by a colon and a backslash, e.g., "C:\", (as on Windows).
func isAbsolutePath(path string) bool {
	isWindowsPath := func(p string) bool {
		if len(p) < 3 {
			return false
		}
		drive := p[0]
		colon := p[1]
		backslash := p[2]
		if drive >= 'A' && drive <= 'Z' && colon == ':' && backslash == '\\' {
			return true
		}
		return false
	}
	return strings.HasPrefix(path, "/") || isWindowsPath(path)
}

// NetworkAddress converts a PostgreSQL host and port into network and address suitable for use with
// net.Dial.
func NetworkAddress(host string, port uint16) (network, address string) {
	if isAbsolutePath(host) {
		network = "unix"
		address = filepath.Join(host, ".s.PGSQL.") + strconv.FormatInt(int64(port), 10)
	} else {
		network = "tcp"
		address = net.JoinHostPort(host, strconv.Itoa(int(port)))
	}
	return network, address
}

// ParseConfig builds a *Config from connString with similar behavior to the PostgreSQL standard C library libpq. It
// uses the same defaults as libpq (e.g. port=5432) and understands most PG* environment variables. ParseConfig closely
// matches the parsing behavior of libpq. connString may either be in URL format or keyword = value format. See
// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING for details. connString also may be empty
// to only read from the environment. If a password is not supplied it will attempt to read the .pgpass file.
//
//	# Example Keyword/Value
//	user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-full
//
//	# Example URL
//	postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-full
//
// The returned *Config may be modified. However, it is strongly recommended that any configuration that can be done
// through the connection string be done there. In particular the fields Host, Port, TLSConfig, and Fallbacks can be
// interdependent (e.g. TLSConfig needs knowledge of the host to validate the server certificate). These fields should
// not be modified individually. They should all be modified or all left unchanged.
//
// ParseConfig supports specifying multiple hosts in similar manner to libpq. Host and port may include comma separated
// values that will be tried in order. This can be used as part of a high availability system. See
// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-MULTIPLE-HOSTS for more information.
//
//	# Example URL
//	postgres://jack:secret@foo.example.com:5432,bar.example.com:5432/mydb
//
// ParseConfig currently recognizes the following environment variable and their parameter key word equivalents passed
// via database URL or keyword/value:
//
//	PGHOST
//	PGPORT
//	PGDATABASE
//	PGUSER
//	PGPASSWORD
//	PGPASSFILE
//	PGSERVICE
//	PGSERVICEFILE
//	PGSSLMODE
//	PGSSLCERT
//	PGSSLKEY
//	PGSSLROOTCERT
//	PGSSLPASSWORD
//	PGOPTIONS
//	PGAPPNAME
//	PGCONNECT_TIMEOUT
//	PGTARGETSESSIONATTRS
//	PGTZ
//	PGMINPROTOCOLVERSION
//	PGMAXPROTOCOLVERSION
//
// See http://www.postgresql.org/docs/current/static/libpq-envars.html for details on the meaning of environment variables.
//
// See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS for parameter key word names. They are
// usually but not always the environment variable name downcased and without the "PG" prefix.
//
// Important Security Notes:
//
// ParseConfig tries to match libpq behavior with regard to PGSSLMODE. This includes defaulting to "prefer" behavior if
// not set.
//
// See http://www.postgresql.org/docs/current/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION for details on what level of
// security each sslmode provides.
//
// The sslmode "prefer" (the default), sslmode "allow", and multiple hosts are implemented via the Fallbacks field of
// the Config struct. If TLSConfig is manually changed it will not affect the fallbacks. For example, in the case of
// sslmode "prefer" this means it will first try the main Config settings which use TLS, then it will try the fallback
// which does not use TLS. This can lead to an unexpected unencrypted connection if the main TLS config is manually
// changed later but the unencrypted fallback is present. Ensure there are no stale fallbacks when manually setting
// TLSConfig.
//
// Several connection parameters cause ParseConfig to read files from the local filesystem: servicefile, passfile,
// sslkey, sslcert, and sslrootcert. Applications that build connection strings from untrusted input must not allow
// these keys to be set by that input. In particular, servicefile (which pgconn accepts in the connection string;
// libpq does not) is read as an INI file whose entries override other connection settings including host, port, and
// sslmode, so an attacker who controls servicefile and service can redirect the connection. If any portion of the
// connection string is externally supplied, use ParseConfigWithOptions and set ParseConfigOptions.ConnStringAllowedKeys
// to an allow-list of the keys that input is expected to supply; any other key in the connection string is then
// rejected before any filesystem access occurs.
//
// Other known differences with libpq:
//
// When multiple hosts are specified, libpq allows them to have different passwords set via the .pgpass file. pgconn
// does not.
//
// URL query parameters that libpq does not recognize cause libpq to fail with an "invalid URI query parameter" error.
// ParseConfig accepts them: they become runtime parameters or pgx-specific options (e.g. pool_max_conns).
//
// Connection strings containing a NUL byte are rejected, in both URI and keyword/value form. libpq cannot encounter
// one because its conninfo strings are NUL-terminated C strings, but a Go string can carry a NUL into the startup
// packet, where it delimits parameters rather than being data. Settings reaching Config by other routes (a service
// file, or direct assignment to Config.RuntimeParams, User, or Database) are not checked here; a NUL in those is
// caught when the startup message is encoded, and Connect fails rather than sending it.
//
// Error messages from ParseConfig avoid quoting the unredacted connection string and attempt to redact recognizable
// password fields, while libpq quotes the failing input verbatim. This redaction is best effort. An invalid connection
// string can be structurally ambiguous, so pgconn cannot guarantee that every password in malformed input will be
// identified or redacted. Applications should not assume that parse errors are safe to expose when connection strings
// may contain secrets.
//
// In addition, ParseConfig accepts the following options:
//
//   - servicefile.
//     libpq only reads servicefile from the PGSERVICEFILE environment variable. ParseConfig accepts servicefile as a
//     part of the connection string.
func ParseConfig(connString string) (*Config, error) {
	var parseConfigOptions ParseConfigOptions
	return ParseConfigWithOptions(connString, parseConfigOptions)
}

// ParseConfigWithOptions builds a *Config from connString and options with similar behavior to the PostgreSQL standard
// C library libpq. options contains settings that cannot be specified in a connString such as providing a function to
// get the SSL password.
func ParseConfigWithOptions(connString string, options ParseConfigOptions) (*Config, error) {
	defaultSettings := defaultSettings()
	envSettings := parseEnvSettings()

	connStringSettings := make(map[string]string)
	var urlMeta parseURLMeta
	if connString != "" {
		var err error
		// connString may be a database URL or in PostgreSQL keyword/value format
		if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
			connStringSettings, urlMeta, err = parseURLSettings(connString)
			if err != nil {
				return nil, &ParseConfigError{ConnString: connString, msg: "failed to parse as URL", err: err}
			}
		} else {
			connStringSettings, err = parseKeywordValueSettings(connString)
			if err != nil {
				return nil, &ParseConfigError{ConnString: connString, msg: "failed to parse as keyword/value", err: err}
			}
		}
	}

	if options.ConnStringAllowedKeys != nil {
		allowed := make(map[string]struct{}, len(options.ConnStringAllowedKeys))
		for _, k := range options.ConnStringAllowedKeys {
			allowed[canonicalConnStringKey(k)] = struct{}{}
		}
		notAllowed := func(k string) error {
			return &ParseConfigError{ConnString: connString, msg: fmt.Sprintf("connection string key %q is not in ConnStringAllowedKeys", k)}
		}
		_, sslAllowed := allowed["ssl"]
		_, sslmodeAllowed := allowed["sslmode"]

		// Repeated-key handling and the URI ssl=true alias rewrite can remove
		// ssl/sslmode occurrences from the final settings map, so those two keys
		// are validated from what the user actually wrote -- every occurrence,
		// fail closed -- rather than from what survived. The alias itself is
		// accepted under either spelling, the same way dbname/database are
		// interchangeable; an explicit sslmode key or a non-"true" ssl value
		// only matches its own spelling.
		if urlMeta.sawRawSSLKey && !sslAllowed {
			return nil, notAllowed("ssl")
		}
		if urlMeta.sawExplicitSSLModeKey && !sslmodeAllowed {
			return nil, notAllowed("sslmode")
		}
		if urlMeta.sawSSLTrueAlias && !sslAllowed && !sslmodeAllowed {
			return nil, notAllowed("ssl")
		}

		for k := range connStringSettings {
			if _, ok := allowed[k]; ok {
				continue
			}
			// A multi-host URI with no explicit ports produces an implied
			// all-empty port list (e.g. ","), matching libpq. Only that list
			// -- identified by the parser from the raw syntax, not inferred
			// from the value -- is exempt from the allow-list: it carries no
			// user-supplied port text. An explicit empty port (?port= in a
			// URI or port= in keyword/value form) is user-supplied, and
			// because a present-but-empty port still shadows PGPORT it must
			// pass the allow-list like any other key.
			if k == "port" && urlMeta.impliedEmptyPortList {
				continue
			}
			// A surviving ssl or sslmode entry from a URI was already
			// validated above against every spelling the user wrote.
			if k == "ssl" && urlMeta.sawRawSSLKey {
				continue
			}
			if k == "sslmode" && (urlMeta.sawSSLTrueAlias || urlMeta.sawExplicitSSLModeKey) {
				continue
			}
			return nil, notAllowed(k)
		}
	}

	settings := mergeSettings(defaultSettings, envSettings, connStringSettings)

	// The home-directory-derived defaults (passfile, servicefile, sslcert,
	// sslkey, sslrootcert) are already present in settings at this point:
	// defaultSettings resolves them via the user's home directory, which is
	// safe and cheap to look up (see defaults.go / defaults_windows.go).
	//
	// The default PostgreSQL user name is different: resolving it requires
	// looking up the OS user account, which can be slow or, in some
	// restricted container environments, crash the process. So that lookup
	// is memoized and only performed lazily below, the first time it is
	// actually needed -- i.e. only when a connection string or environment
	// does not already supply a user.
	var cachedOSUserSettings map[string]string
	lazyOSUserSettings := func() map[string]string {
		if cachedOSUserSettings == nil {
			cachedOSUserSettings = osUserSettings()
		}
		return cachedOSUserSettings
	}

	if service, present := settings["service"]; present {
		serviceSettings, err := parseServiceSettings(settings["servicefile"], service)
		if err != nil {
			return nil, &ParseConfigError{ConnString: connString, msg: "failed to read service", err: err}
		}

		settings = mergeSettings(defaultSettings, envSettings, serviceSettings, connStringSettings)
	}

	// Only fall back to the OS user account for the default PostgreSQL user
	// name when it was not already supplied by the connection string,
	// environment, or service file.
	if settings["user"] == "" {
		settings = mergeSettings(lazyOSUserSettings(), settings)
	}

	config := &Config{
		createdByParseConfig: true,
		Database:             settings["database"],
		User:                 settings["user"],
		Password:             settings["password"],
		RuntimeParams:        make(map[string]string),
		BuildFrontend:        pgproto3.NewFrontend,
		BuildContextWatcherHandler: func(pgConn *PgConn) ctxwatch.Handler {
			return &DeadlineContextWatcherHandler{Conn: pgConn.conn}
		},
		OnPgError: func(_ *PgConn, pgErr *PgError) bool {
			// we want to automatically close any fatal errors
			if strings.EqualFold(pgErr.Severity, "FATAL") {
				return false
			}
			return true
		},
	}

	if connectTimeoutSetting, present := settings["connect_timeout"]; present {
		connectTimeout, err := parseConnectTimeoutSetting(connectTimeoutSetting)
		if err != nil {
			return nil, &ParseConfigError{ConnString: connString, msg: "invalid connect_timeout", err: err}
		}
		config.ConnectTimeout = connectTimeout
		config.DialFunc = makeConnectTimeoutDialFunc(connectTimeout)
	} else {
		defaultDialer := makeDefaultDialer()
		config.DialFunc = defaultDialer.DialContext
	}

	config.LookupFunc = makeDefaultResolver().LookupHost

	notRuntimeParams := map[string]struct{}{
		"host":                 {},
		"port":                 {},
		"database":             {},
		"user":                 {},
		"password":             {},
		"passfile":             {},
		"connect_timeout":      {},
		"sslmode":              {},
		"sslkey":               {},
		"sslcert":              {},
		"sslrootcert":          {},
		"sslnegotiation":       {},
		"sslpassword":          {},
		"sslsni":               {},
		"krbspn":               {},
		"krbsrvname":           {},
		"target_session_attrs": {},
		"service":              {},
		"servicefile":          {},
		"min_protocol_version": {},
		"max_protocol_version": {},
		"channel_binding":      {},
		"require_auth":         {},
	}

	// Adding kerberos configuration
	if _, present := settings["krbsrvname"]; present {
		config.KerberosSrvName = settings["krbsrvname"]
	}
	if _, present := settings["krbspn"]; present {
		config.KerberosSpn = settings["krbspn"]
	}

	for k, v := range settings {
		if _, present := notRuntimeParams[k]; present {
			continue
		}
		config.RuntimeParams[k] = v
	}

	fallbacks := []*FallbackConfig{}

	hosts := strings.Split(settings["host"], ",")
	ports := strings.Split(settings["port"], ",")

	// Like libpq, if exactly one port is given it applies to all hosts;
	// otherwise there must be exactly one port per host. Empty list elements
	// mean "use the default".
	if len(ports) > 1 && len(ports) != len(hosts) {
		return nil, &ParseConfigError{ConnString: connString, msg: fmt.Sprintf("could not match %d port numbers to %d hosts", len(ports), len(hosts))}
	}

	// defaultHost stats candidate socket directories, so resolve it at most
	// once even when several host list elements are empty. It never returns "".
	resolvedDefaultHost := ""
	for i, host := range hosts {
		if host == "" {
			if resolvedDefaultHost == "" {
				resolvedDefaultHost = defaultHost()
			}
			host = resolvedDefaultHost
		}

		portStr := ports[0]
		if len(ports) > 1 {
			portStr = ports[i]
		}
		if portStr == "" {
			portStr = defaultPort
		}

		// The strconv error is deliberately not wrapped: it quotes the
		// offending text, and in a malformed URI the bytes that land in the
		// port position can be a mislaid password (postgres://u:sec:ret@h
		// parses "sec:ret@h" as the port). The best-effort-redacted connection
		// string in ParseConfigError provides the available context without
		// deliberately quoting the offending port text again.
		port, err := parsePort(portStr)
		if err != nil {
			return nil, &ParseConfigError{ConnString: connString, msg: "invalid port"}
		}

		var tlsConfigs []*tls.Config

		// Ignore TLS settings if Unix domain socket like libpq
		if network, _ := NetworkAddress(host, port); network == "unix" {
			tlsConfigs = append(tlsConfigs, nil)
		} else {
			var err error
			tlsConfigs, err = configTLS(settings, host, options)
			if err != nil {
				return nil, &ParseConfigError{ConnString: connString, msg: "failed to configure TLS", err: err}
			}
		}

		for _, tlsConfig := range tlsConfigs {
			fallbacks = append(fallbacks, &FallbackConfig{
				Host:      host,
				Port:      port,
				TLSConfig: tlsConfig,
			})
		}
	}

	config.Host = fallbacks[0].Host
	config.Port = fallbacks[0].Port
	config.TLSConfig = fallbacks[0].TLSConfig
	config.Fallbacks = fallbacks[1:]
	config.SSLNegotiation = settings["sslnegotiation"]

	if config.Password == "" {
		passfile, err := pgpassfile.ReadPassfile(settings["passfile"])
		if err == nil {
			host := config.Host
			if network, _ := NetworkAddress(config.Host, config.Port); network == "unix" {
				host = "localhost"
			}
			config.Password = passfile.FindPassword(host, strconv.Itoa(int(config.Port)), config.Database, config.User)
		}
	}

	switch tsa := settings["target_session_attrs"]; tsa {
	case "read-write":
		config.ValidateConnect = ValidateConnectTargetSessionAttrsReadWrite
	case "read-only":
		config.ValidateConnect = ValidateConnectTargetSessionAttrsReadOnly
	case "primary":
		config.ValidateConnect = ValidateConnectTargetSessionAttrsPrimary
	case "standby":
		config.ValidateConnect = ValidateConnectTargetSessionAttrsStandby
	case "prefer-standby":
		config.ValidateConnect = ValidateConnectTargetSessionAttrsPreferStandby
	case "any":
		// do nothing
	default:
		return nil, &ParseConfigError{ConnString: connString, msg: fmt.Sprintf("unknown target_session_attrs value: %v", tsa)}
	}

	minProto, err := parseProtocolVersion(settings["min_protocol_version"])
	if err != nil {
		return nil, &ParseConfigError{ConnString: connString, msg: fmt.Sprintf("invalid min_protocol_version: %q", settings["min_protocol_version"]), err: err}
	}
	maxProto, err := parseProtocolVersion(settings["max_protocol_version"])
	if err != nil {
		return nil, &ParseConfigError{ConnString: connString, msg: fmt.Sprintf("invalid max_protocol_version: %q", settings["max_protocol_version"]), err: err}
	}

	config.MinProtocolVersion = settings["min_protocol_version"]
	config.MaxProtocolVersion = settings["max_protocol_version"]

	if config.MinProtocolVersion == "" {
		config.MinProtocolVersion = "3.0"
	}

	// When max_protocol_version is not explicitly set, default based on
	// min_protocol_version. This matches libpq behavior: if min > 3.0,
	// default max to latest; otherwise default to 3.0 for compatibility
	// with older servers/poolers that don't support NegotiateProtocolVersion.
	if config.MaxProtocolVersion == "" {
		if minProto > pgproto3.ProtocolVersion30 {
			config.MaxProtocolVersion = "latest"
		} else {
			config.MaxProtocolVersion = "3.0"
		}
	}

	// Only error when max_protocol_version was explicitly set and conflicts
	// with min_protocol_version. When max_protocol_version is not explicitly
	// set, the auto-raise logic above already ensures a valid default.
	if minProto > maxProto && settings["max_protocol_version"] != "" {
		return nil, &ParseConfigError{ConnString: connString, msg: "min_protocol_version cannot be greater than max_protocol_version"}
	}

	switch channelBinding := settings["channel_binding"]; channelBinding {
	case "", "prefer":
		config.ChannelBinding = "prefer"
	case "disable":
		config.ChannelBinding = "disable"
	case "require":
		config.ChannelBinding = "require"
	default:
		return nil, &ParseConfigError{ConnString: connString, msg: fmt.Sprintf("unknown channel_binding value: %v", channelBinding)}
	}

	config.RequireAuth = settings["require_auth"]
	if _, err := parseRequireAuth(config.RequireAuth); err != nil {
		return nil, &ParseConfigError{ConnString: connString, msg: "invalid require_auth", err: err}
	}

	return config, nil
}

func mergeSettings(settingSets ...map[string]string) map[string]string {
	settings := make(map[string]string)

	for _, s2 := range settingSets {
		maps.Copy(settings, s2)
	}

	return settings
}

func parseEnvSettings() map[string]string {
	settings := make(map[string]string)

	nameMap := map[string]string{
		"PGHOST":               "host",
		"PGPORT":               "port",
		"PGDATABASE":           "database",
		"PGUSER":               "user",
		"PGPASSWORD":           "password",
		"PGPASSFILE":           "passfile",
		"PGAPPNAME":            "application_name",
		"PGCONNECT_TIMEOUT":    "connect_timeout",
		"PGSSLMODE":            "sslmode",
		"PGSSLKEY":             "sslkey",
		"PGSSLCERT":            "sslcert",
		"PGSSLSNI":             "sslsni",
		"PGSSLROOTCERT":        "sslrootcert",
		"PGSSLPASSWORD":        "sslpassword",
		"PGSSLNEGOTIATION":     "sslnegotiation",
		"PGTARGETSESSIONATTRS": "target_session_attrs",
		"PGSERVICE":            "service",
		"PGSERVICEFILE":        "servicefile",
		"PGTZ":                 "timezone",
		"PGOPTIONS":            "options",
		"PGMINPROTOCOLVERSION": "min_protocol_version",
		"PGMAXPROTOCOLVERSION": "max_protocol_version",
		"PGCHANNELBINDING":     "channel_binding",
		"PGREQUIREAUTH":        "require_auth",
	}

	for envname, realname := range nameMap {
		value := os.Getenv(envname)
		if value != "" {
			settings[realname] = value
		}
	}

	return settings
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func parseKeywordValueSettings(s string) (map[string]string, error) {
	settings := make(map[string]string)

	// Reject NUL bytes up front, as parseURLSettings does. libpq never sees one
	// because its conninfo strings are NUL-terminated C strings; a Go string can
	// carry a NUL through to the startup packet, where it acts as a parameter
	// delimiter rather than data. StartupMessage.Encode refuses such parameters,
	// but failing here reports the problem against the input that caused it.
	if strings.IndexByte(s, 0) >= 0 {
		return nil, errors.New("forbidden NUL byte in connection string")
	}

	// Trim any leading whitespace so that the loop exits cleanly when only
	// spaces remain (e.g. trailing spaces after the last value).
	s = strings.TrimLeft(s, " \t\n\r\v\f")
	for len(s) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(s, '=')
		if eqIdx < 0 {
			return nil, errors.New("invalid keyword/value")
		}

		key = strings.Trim(s[:eqIdx], " \t\n\r\v\f")
		s = strings.TrimLeft(s[eqIdx+1:], " \t\n\r\v\f")
		switch {
		case len(s) == 0:
		case s[0] != '\'':
			end := 0
			for ; end < len(s); end++ {
				if asciiSpace[s[end]] == 1 {
					break
				}
				if s[end] == '\\' {
					end++
					if end == len(s) {
						return nil, errors.New("invalid backslash")
					}
				}
			}
			val = strings.ReplaceAll(strings.ReplaceAll(s[:end], "\\\\", "\\"), "\\'", "'")
			// Consume the value and trim any subsequent whitespace so that
			// multiple trailing spaces don't cause a spurious parse failure.
			s = strings.TrimLeft(s[end:], " \t\n\r\v\f")
		default: // quoted string
			s = s[1:]
			end := 0
			for ; end < len(s); end++ {
				if s[end] == '\'' {
					break
				}
				if s[end] == '\\' {
					end++
				}
			}
			if end == len(s) {
				return nil, errors.New("unterminated quoted string in connection info string")
			}
			val = strings.ReplaceAll(strings.ReplaceAll(s[:end], "\\\\", "\\"), "\\'", "'")
			// Consume the closing quote and any subsequent whitespace.
			s = strings.TrimLeft(s[end+1:], " \t\n\r\v\f")
		}

		key = canonicalConnStringKey(key)

		if key == "" {
			return nil, errors.New("invalid keyword/value")
		}

		if key == "user" && val == "" {
			continue
		}
		settings[key] = val
	}

	return settings, nil
}

func parseServiceSettings(servicefilePath, serviceName string) (map[string]string, error) {
	servicefile, err := pgservicefile.ReadServicefile(servicefilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read service file: %v", servicefilePath)
	}

	service, err := servicefile.GetService(serviceName)
	if err != nil {
		return nil, fmt.Errorf("unable to find service: %v", serviceName)
	}

	settings := make(map[string]string, len(service.Settings))
	for k, v := range service.Settings {
		settings[canonicalConnStringKey(k)] = v
	}

	return settings, nil
}

// configTLS uses libpq's TLS parameters to construct  []*tls.Config. It is
// necessary to allow returning multiple TLS configs as sslmode "allow" and
// "prefer" allow fallback.
func configTLS(settings map[string]string, thisHost string, parseConfigOptions ParseConfigOptions) ([]*tls.Config, error) {
	host := thisHost
	sslmode := settings["sslmode"]
	sslrootcert := settings["sslrootcert"]
	sslcert := settings["sslcert"]
	sslkey := settings["sslkey"]
	sslpassword := settings["sslpassword"]
	sslsni := settings["sslsni"]
	sslnegotiation := settings["sslnegotiation"]

	// Match libpq default behavior
	if sslmode == "" {
		sslmode = "prefer"
	}
	if sslsni == "" {
		sslsni = "1"
	}

	tlsConfig := &tls.Config{}

	if sslnegotiation == "direct" {
		tlsConfig.NextProtos = []string{"postgresql"}
		if sslmode == "prefer" {
			sslmode = "require"
		}
	}

	if sslrootcert != "" {
		var caCertPool *x509.CertPool

		if sslrootcert == "system" {
			var err error

			caCertPool, err = x509.SystemCertPool()
			if err != nil {
				return nil, fmt.Errorf("unable to load system certificate pool: %w", err)
			}

			sslmode = "verify-full"
		} else {
			caCertPool = x509.NewCertPool()

			caPath := sslrootcert
			caCert, err := os.ReadFile(caPath)
			if err != nil {
				return nil, fmt.Errorf("unable to read CA file: %w", err)
			}

			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, errors.New("unable to add CA to cert pool")
			}
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow", "prefer":
		tlsConfig.InsecureSkipVerify = true
	case "require":
		// According to PostgreSQL documentation, if a root CA file exists,
		// the behavior of sslmode=require should be the same as that of verify-ca
		//
		// See https://www.postgresql.org/docs/current/libpq-ssl.html
		if sslrootcert != "" {
			goto nextCase
		}
		tlsConfig.InsecureSkipVerify = true
		break
	nextCase:
		fallthrough
	case "verify-ca":
		// Don't perform the default certificate verification because it
		// will verify the hostname. Instead, verify the server's
		// certificate chain ourselves in VerifyPeerCertificate and
		// ignore the server name. This emulates libpq's verify-ca
		// behavior.
		//
		// See https://github.com/golang/go/issues/21971#issuecomment-332693931
		// and https://pkg.go.dev/crypto/tls?tab=doc#example-Config-VerifyPeerCertificate
		// for more info.
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyPeerCertificate = func(certificates [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, len(certificates))
			for i, asn1Data := range certificates {
				cert, err := x509.ParseCertificate(asn1Data)
				if err != nil {
					return errors.New("failed to parse certificate from server: " + err.Error())
				}
				certs[i] = cert
			}

			// Leave DNSName empty to skip hostname verification.
			opts := x509.VerifyOptions{
				Roots:         tlsConfig.RootCAs,
				Intermediates: x509.NewCertPool(),
			}
			// Skip the first cert because it's the leaf. All others
			// are intermediates.
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := certs[0].Verify(opts)
			return err
		}
	case "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, errors.New("sslmode is invalid")
	}

	if (sslcert != "" && sslkey == "") || (sslcert == "" && sslkey != "") {
		return nil, errors.New(`both "sslcert" and "sslkey" are required`)
	}

	if sslcert != "" && sslkey != "" {
		buf, err := os.ReadFile(sslkey)
		if err != nil {
			return nil, fmt.Errorf("unable to read sslkey: %w", err)
		}
		block, _ := pem.Decode(buf)
		if block == nil {
			return nil, errors.New("failed to decode sslkey")
		}
		var pemKey []byte
		var decryptedKey []byte
		var decryptedError error
		// If PEM is encrypted, attempt to decrypt using pass phrase
		if x509.IsEncryptedPEMBlock(block) {
			// Attempt decryption with pass phrase
			// NOTE: only supports RSA (PKCS#1)
			if sslpassword != "" {
				decryptedKey, decryptedError = x509.DecryptPEMBlock(block, []byte(sslpassword)) //nolint:ineffassign
			}
			// if sslpassword not provided or has decryption error when use it
			// try to find sslpassword with callback function
			if sslpassword == "" || decryptedError != nil {
				if parseConfigOptions.GetSSLPassword != nil {
					sslpassword = parseConfigOptions.GetSSLPassword(context.Background())
				}
				if sslpassword == "" {
					return nil, fmt.Errorf("unable to find sslpassword")
				}
			}
			decryptedKey, decryptedError = x509.DecryptPEMBlock(block, []byte(sslpassword))
			// Should we also provide warning for PKCS#1 needed?
			if decryptedError != nil {
				return nil, fmt.Errorf("unable to decrypt key: %w", decryptedError)
			}

			pemBytes := pem.Block{
				Type:  "RSA PRIVATE KEY",
				Bytes: decryptedKey,
			}
			pemKey = pem.EncodeToMemory(&pemBytes)
		} else {
			pemKey = pem.EncodeToMemory(block)
		}
		certfile, err := os.ReadFile(sslcert)
		if err != nil {
			return nil, fmt.Errorf("unable to read cert: %w", err)
		}
		cert, err := tls.X509KeyPair(certfile, pemKey)
		if err != nil {
			return nil, fmt.Errorf("unable to load cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Set Server Name Indication (SNI), if enabled by connection parameters.
	// Per RFC 6066, do not set it if the host is a literal IP address (IPv4
	// or IPv6).
	if sslsni == "1" && net.ParseIP(host) == nil {
		tlsConfig.ServerName = host
	}

	switch sslmode {
	case "allow":
		return []*tls.Config{nil, tlsConfig}, nil
	case "prefer":
		return []*tls.Config{tlsConfig, nil}, nil
	case "require", "verify-ca", "verify-full":
		return []*tls.Config{tlsConfig}, nil
	default:
		panic("BUG: bad sslmode should already have been caught")
	}
}

func parsePort(s string) (uint16, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > math.MaxUint16 {
		return 0, errors.New("outside range")
	}
	return uint16(port), nil
}

func makeDefaultDialer() *net.Dialer {
	// rely on GOLANG KeepAlive settings
	return &net.Dialer{}
}

func makeDefaultResolver() *net.Resolver {
	return net.DefaultResolver
}

func parseConnectTimeoutSetting(s string) (time.Duration, error) {
	timeout, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if timeout < 0 {
		return 0, errors.New("negative timeout")
	}
	return time.Duration(timeout) * time.Second, nil
}

func makeConnectTimeoutDialFunc(timeout time.Duration) DialFunc {
	d := makeDefaultDialer()
	d.Timeout = timeout
	return d.DialContext
}

var (
	// ErrReadOnlyConnection is returned when a read-write connection is required but the connection is read-only.
	ErrReadOnlyConnection = errors.New("read only connection")
	// ErrReadWriteConnection is returned when a read-only connection is required but the connection is read-write.
	ErrReadWriteConnection = errors.New("connection is not read only")
	// ErrPrimaryConnection is returned when a standby connection is required but the server is primary.
	ErrPrimaryConnection = errors.New("server is not in hot standby mode")
	// ErrStandbyConnection is returned when a primary connection is required but the server is in standby mode.
	ErrStandbyConnection = errors.New("server is in standby mode")
)

// ValidateConnectTargetSessionAttrsReadWrite is a ValidateConnectFunc that implements libpq compatible
// target_session_attrs=read-write.
func ValidateConnectTargetSessionAttrsReadWrite(ctx context.Context, pgConn *PgConn) error {
	result, err := pgConn.Exec(ctx, "show transaction_read_only").ReadAll()
	if err != nil {
		return err
	}

	if string(result[0].Rows[0][0]) == "on" {
		return ErrReadOnlyConnection
	}

	return nil
}

// ValidateConnectTargetSessionAttrsReadOnly is a ValidateConnectFunc that implements libpq compatible
// target_session_attrs=read-only.
func ValidateConnectTargetSessionAttrsReadOnly(ctx context.Context, pgConn *PgConn) error {
	result, err := pgConn.Exec(ctx, "show transaction_read_only").ReadAll()
	if err != nil {
		return err
	}

	if string(result[0].Rows[0][0]) != "on" {
		return ErrReadWriteConnection
	}

	return nil
}

// ValidateConnectTargetSessionAttrsStandby is a ValidateConnectFunc that implements libpq compatible
// target_session_attrs=standby.
func ValidateConnectTargetSessionAttrsStandby(ctx context.Context, pgConn *PgConn) error {
	result, err := pgConn.Exec(ctx, "select pg_is_in_recovery()").ReadAll()
	if err != nil {
		return err
	}

	if string(result[0].Rows[0][0]) != "t" {
		return ErrPrimaryConnection
	}

	return nil
}

// ValidateConnectTargetSessionAttrsPrimary is a ValidateConnectFunc that implements libpq compatible
// target_session_attrs=primary.
func ValidateConnectTargetSessionAttrsPrimary(ctx context.Context, pgConn *PgConn) error {
	result, err := pgConn.Exec(ctx, "select pg_is_in_recovery()").ReadAll()
	if err != nil {
		return err
	}

	if string(result[0].Rows[0][0]) == "t" {
		return ErrStandbyConnection
	}

	return nil
}

// ValidateConnectTargetSessionAttrsPreferStandby is a ValidateConnectFunc that implements libpq compatible
// target_session_attrs=prefer-standby.
func ValidateConnectTargetSessionAttrsPreferStandby(ctx context.Context, pgConn *PgConn) error {
	result, err := pgConn.Exec(ctx, "select pg_is_in_recovery()").ReadAll()
	if err != nil {
		return err
	}

	if string(result[0].Rows[0][0]) != "t" {
		return &NotPreferredError{err: ErrPrimaryConnection}
	}

	return nil
}

func parseProtocolVersion(s string) (uint32, error) {
	switch s {
	case "", "3.0":
		return pgproto3.ProtocolVersion30, nil
	case "3.2", "latest":
		return pgproto3.ProtocolVersion32, nil
	default:
		return 0, fmt.Errorf("invalid protocol version: %q", s)
	}
}
