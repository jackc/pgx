package pgconn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/pgpassfile"
	"github.com/pkg/errors"
)

type AcceptConnFunc func(pgconn *PgConn) bool

// Config is the settings used to establish a connection to a PostgreSQL server.
type Config struct {
	Host          string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port          uint16
	Database      string
	User          string
	Password      string
	TLSConfig     *tls.Config       // nil disables TLS
	DialFunc      DialFunc          // e.g. net.Dialer.DialContext
	RuntimeParams map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)

	Fallbacks []*FallbackConfig

	// AcceptConnFunc is called after successful connection allow custom logic for determining if the connection is
	// acceptable. If AcceptConnFunc returns false the connection is closed and the next fallback config is tried. This
	// allows implementing high availability behavior such as libpq does with target_session_attrs.
	AcceptConnFunc AcceptConnFunc
}

// FallbackConfig is additional settings to attempt a connection with when the primary Config fails to establish a
// network connection. It is used for TLS fallback such as sslmode=prefer and high availability (HA) connections.
type FallbackConfig struct {
	Host      string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port      uint16
	TLSConfig *tls.Config // nil disables TLS
}

// NetworkAddress converts a PostgreSQL host and port into network and address suitable for use with
// net.Dial.
func NetworkAddress(host string, port uint16) (network, address string) {
	if strings.HasPrefix(host, "/") {
		network = "unix"
		address = filepath.Join(host, ".s.PGSQL.") + strconv.FormatInt(int64(port), 10)
	} else {
		network = "tcp"
		address = fmt.Sprintf("%s:%d", host, port)
	}
	return network, address
}

// ParseConfig builds a []*Config with similar behavior to the PostgreSQL standard C library libpq. It uses the same
// defaults as libpq (e.g. port=5432) and understands most PG* environment variables. connString may be a URL or a DSN.
// It also may be empty to only read from the environment. If a password is not supplied it will attempt to read the
// .pgpass file.
//
// Example DSN: "user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca"
//
// Example URL: "postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca"
//
// ParseConfig supports specifying multiple hosts in similar manner to libpq. Host and port may include comma separated
// values that will be tried in order. This can be used as part of a high availability system. See
// https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-MULTIPLE-HOSTS for more information.
//
// Example URL: "postgres://jack:secret@foo.example.com:5432,bar.example.com:5432/mydb"
//
// ParseConfig currently recognizes the following environment variable and their parameter key word equivalents passed
// via database URL or DSN:
//
// PGHOST
// PGPORT
// PGDATABASE
// PGUSER
// PGPASSWORD
// PGPASSFILE
// PGSSLMODE
// PGSSLCERT
// PGSSLKEY
// PGSSLROOTCERT
// PGAPPNAME
// PGCONNECT_TIMEOUT
//
// See http://www.postgresql.org/docs/11/static/libpq-envars.html for details on the meaning of environment variables.
//
// See https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS for parameter key word names. They are
// usually but not always the environment variable name downcased and without the "PG" prefix.
//
// Important TLS Security Notes:
//
// ParseConfig tries to match libpq behavior with regard to PGSSLMODE. This includes defaulting to "prefer" behavior if
// not set.
//
// See http://www.postgresql.org/docs/11/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION for details on what level of
// security each sslmode provides.
//
// "verify-ca" mode currently is treated as "verify-full". e.g. It has stronger
// security guarantees than it would with libpq. Do not rely on this behavior as it
// may be possible to match libpq in the future. If you need full security use
// "verify-full".
func ParseConfig(connString string) (*Config, error) {
	settings := defaultSettings()
	addEnvSettings(settings)

	if connString != "" {
		// connString may be a database URL or a DSN
		if strings.HasPrefix(connString, "postgres://") {
			err := addURLSettings(settings, connString)
			if err != nil {
				return nil, err
			}
		} else {
			err := addDSNSettings(settings, connString)
			if err != nil {
				return nil, err
			}
		}
	}

	config := &Config{
		Database:      settings["database"],
		User:          settings["user"],
		Password:      settings["password"],
		RuntimeParams: make(map[string]string),
	}

	if connectTimeout, present := settings["connect_timeout"]; present {
		dialFunc, err := makeConnectTimeoutDialFunc(connectTimeout)
		if err != nil {
			return nil, err
		}
		config.DialFunc = dialFunc
	} else {
		defaultDialer := makeDefaultDialer()
		config.DialFunc = defaultDialer.DialContext
	}

	notRuntimeParams := map[string]struct{}{
		"host":            struct{}{},
		"port":            struct{}{},
		"database":        struct{}{},
		"user":            struct{}{},
		"password":        struct{}{},
		"passfile":        struct{}{},
		"connect_timeout": struct{}{},
		"sslmode":         struct{}{},
		"sslkey":          struct{}{},
		"sslcert":         struct{}{},
		"sslrootcert":     struct{}{},
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

	for i, host := range hosts {
		var portStr string
		if i < len(ports) {
			portStr = ports[i]
		} else {
			portStr = ports[0]
		}

		port, err := parsePort(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %v", settings["port"])
		}

		var tlsConfigs []*tls.Config

		// Ignore TLS settings if Unix domain socket like libpq
		if network, _ := NetworkAddress(host, port); network == "unix" {
			tlsConfigs = append(tlsConfigs, nil)
		} else {
			var err error
			tlsConfigs, err = configTLS(settings)
			if err != nil {
				return nil, err
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

	passfile, err := pgpassfile.ReadPassfile(settings["passfile"])
	if err == nil {
		if config.Password == "" {
			host := config.Host
			if network, _ := NetworkAddress(config.Host, config.Port); network == "unix" {
				host = "localhost"
			}

			config.Password = passfile.FindPassword(host, strconv.Itoa(int(config.Port)), config.Database, config.User)
		}
	}

	return config, nil
}

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = defaultHost()
	settings["port"] = "5432"

	// Default to the OS user name. Purposely ignoring err getting user name from
	// OS. The client application will simply have to specify the user in that
	// case (which they typically will be doing anyway).
	user, err := user.Current()
	if err == nil {
		settings["user"] = user.Username
		settings["passfile"] = filepath.Join(user.HomeDir, ".pgpass")
	}

	return settings
}

// defaultHost attempts to mimic libpq's default host. libpq uses the default unix socket location on *nix and localhost
// on Windows. The default socket location is compiled into libpq. Since pgx does not have access to that default it
// checks the existence of common locations.
func defaultHost() string {
	candidatePaths := []string{
		"/var/run/postgresql", // Debian
		"/private/tmp",        // OSX - homebrew
		"/tmp",                // standard PostgreSQL
	}

	for _, path := range candidatePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "localhost"
}

func addEnvSettings(settings map[string]string) {
	nameMap := map[string]string{
		"PGHOST":            "host",
		"PGPORT":            "port",
		"PGDATABASE":        "database",
		"PGUSER":            "user",
		"PGPASSWORD":        "password",
		"PGPASSFILE":        "passfile",
		"PGAPPNAME":         "application_name",
		"PGCONNECT_TIMEOUT": "connect_timeout",
		"PGSSLMODE":         "sslmode",
		"PGSSLKEY":          "sslkey",
		"PGSSLCERT":         "sslcert",
		"PGSSLROOTCERT":     "sslrootcert",
	}

	for envname, realname := range nameMap {
		value := os.Getenv(envname)
		if value != "" {
			settings[realname] = value
		}
	}
}

func addURLSettings(settings map[string]string, connString string) error {
	url, err := url.Parse(connString)
	if err != nil {
		return err
	}

	if url.User != nil {
		settings["user"] = url.User.Username()
		if password, present := url.User.Password(); present {
			settings["password"] = password
		}
	}

	// Handle multiple host:port's in url.Host by splitting them into host,host,host and port,port,port.
	var hosts []string
	var ports []string
	for _, host := range strings.Split(url.Host, ",") {
		parts := strings.SplitN(host, ":", 2)
		if parts[0] != "" {
			hosts = append(hosts, parts[0])
		}
		if len(parts) == 2 {
			ports = append(ports, parts[1])
		}
	}
	if len(hosts) > 0 {
		settings["host"] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		settings["port"] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(url.Path, "/")
	if database != "" {
		settings["database"] = database
	}

	for k, v := range url.Query() {
		settings[k] = v[0]
	}

	return nil
}

var dsnRegexp = regexp.MustCompile(`([a-zA-Z_]+)=((?:"[^"]+")|(?:[^ ]+))`)

func addDSNSettings(settings map[string]string, s string) error {
	m := dsnRegexp.FindAllStringSubmatch(s, -1)

	for _, b := range m {
		settings[b[1]] = b[2]
	}

	return nil
}

type pgTLSArgs struct {
	sslMode     string
	sslRootCert string
	sslCert     string
	sslKey      string
}

// configTLS uses libpq's TLS parameters to construct  []*tls.Config. It is
// necessary to allow returning multiple TLS configs as sslmode "allow" and
// "prefer" allow fallback.
func configTLS(settings map[string]string) ([]*tls.Config, error) {
	host := settings["host"]
	sslmode := settings["sslmode"]
	sslrootcert := settings["sslrootcert"]
	sslcert := settings["sslcert"]
	sslkey := settings["sslkey"]

	// Match libpq default behavior
	if sslmode == "" {
		sslmode = "prefer"
	}

	tlsConfig := &tls.Config{}

	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow", "prefer":
		tlsConfig.InsecureSkipVerify = true
	case "require":
		tlsConfig.InsecureSkipVerify = sslrootcert == ""
	case "verify-ca", "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, errors.New("sslmode is invalid")
	}

	if sslrootcert != "" {
		caCertPool := x509.NewCertPool()

		caPath := sslrootcert
		caCert, err := ioutil.ReadFile(caPath)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read CA file %q", caPath)
		}

		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.Wrap(err, "unable to add CA to cert pool")
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	if (sslcert != "" && sslkey == "") || (sslcert == "" && sslkey != "") {
		return nil, fmt.Errorf(`both "sslcert" and "sslkey" are required`)
	}

	if sslcert != "" && sslkey != "" {
		cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read cert")
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
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
	return &net.Dialer{KeepAlive: 5 * time.Minute}
}

func makeConnectTimeoutDialFunc(s string) (DialFunc, error) {
	timeout, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, errors.New("negative timeout")
	}

	d := makeDefaultDialer()
	d.Timeout = time.Duration(timeout) * time.Second
	return d.DialContext, nil
}
