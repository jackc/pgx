package pgconn

import (
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgpassfile"
	"github.com/jackc/pgx/v5/pgproto3"
)

type logicalHost struct {
	name, address, password string
	port                    uint16
	passwordProvided        bool
}
type connectSettings struct {
	values        map[string]string
	hosts         []logicalHost
	timeout       time.Duration
	options       ConnectOptions2
	clientOptions map[string]string
	random        *mathrand.Rand // optional deterministic source for protocol fixtures
}

var (
	connect2Keys            = strings.Fields(`host hostaddr port database user password passfile connect_timeout sslmode sslnegotiation sslcert sslkey sslrootcert sslpassword sslsni ssl_min_protocol_version ssl_max_protocol_version target_session_attrs load_balance_hosts channel_binding require_auth min_protocol_version max_protocol_version service servicefile options application_name fallback_application_name client_encoding timezone krbsrvname krbspn statement_cache_capacity description_cache_capacity default_query_exec_mode`)
	unsupportedConnect2Keys = strings.Fields(`keepalives keepalives_idle keepalives_interval keepalives_count tcp_user_timeout requirepeer sslcompression sslcertmode sslcrl sslcrldir sslkeylogfile gssencmode gsslib gssdelegation replication requirepass scram_client_key scram_server_key oauth_issuer oauth_client_id oauth_client_secret oauth_scope`)
)

func validateConnect2Keys(values map[string]string) error {
	for k, v := range values {
		if strings.IndexByte(v, 0) >= 0 {
			return errors.New("NUL in connection parameter")
		}
		if slices.Contains(unsupportedConnect2Keys, k) {
			return fmt.Errorf("unsupported connection parameter %q", k)
		}
		if !slices.Contains(connect2Keys, k) {
			return fmt.Errorf("unknown connection parameter %q; use options for server settings", k)
		}
	}
	return nil
}

func resolveConnectSettings(connString string, opts ConnectOptions2) (*connectSettings, error) {
	// Check every untrusted key before defaultSettings performs filesystem access.
	allowed := opts.ConnStringAllowedKeys
	if allowed == nil {
		allowed = append(slices.Clone(connect2Keys), unsupportedConnect2Keys...)
	}

	if opts.ConnStringAllowedKeys != nil && !strings.HasPrefix(connString, "postgres://") && !strings.HasPrefix(connString, "postgresql://") {
		allowedSet := make(map[string]bool, len(opts.ConnStringAllowedKeys))
		for _, k := range opts.ConnStringAllowedKeys {
			allowedSet[canonicalConnStringKey(k)] = true
		}
		var keyError error
		if _, err := parseKeywordValueSettingsChecked(connString, func(k string) error {
			if !allowedSet[k] {
				keyError = fmt.Errorf("connection string key %q is not in ConnStringAllowedKeys", k)
				return keyError
			}
			return nil
		}); err != nil {
			if keyError != nil {
				return nil, keyError
			}
			return nil, errors.New("invalid connection string syntax")
		}
	}
	explicit, err := parseConnectionString(connString, allowed)
	if err != nil {
		// The new API never retains or formats the original credential-bearing
		// string in an exported error. Parser details may quote malformed values.
		var parseErr *ParseConfigError
		if errors.As(err, &parseErr) {
			return nil, errors.New(parseErr.msg)
		}
		return nil, errors.New("invalid connection string")
	}
	if err = validateConnect2Keys(explicit); err != nil {
		return nil, err
	}
	defaults, env := defaultSettings(), parseEnvSettings()
	for k, v := range map[string]string{"PGHOSTADDR": "hostaddr", "PGLOADBALANCEHOSTS": "load_balance_hosts", "PGCLIENTENCODING": "client_encoding", "PGSSLMINPROTOCOLVERSION": "ssl_min_protocol_version", "PGSSLMAXPROTOCOLVERSION": "ssl_max_protocol_version"} {
		if s := os.Getenv(k); s != "" {
			env[v] = s
		}
	}
	for _, k := range unsupportedConnect2Keys {
		if v := os.Getenv("PG" + strings.ToUpper(strings.ReplaceAll(k, "_", ""))); v != "" {
			env[k] = v
		}
	}
	inputs := mergeSettings(env, explicit)
	if name := inputs["service"]; name != "" {
		path := inputs["servicefile"]
		if path == "" {
			path = defaults["servicefile"]
			if _, e := os.Stat(path); os.IsNotExist(e) && os.Getenv("PGSYSCONFDIR") != "" {
				path = filepath.Join(os.Getenv("PGSYSCONFDIR"), "pg_service.conf")
			}
		}
		service, e := parseServiceSettings(path, name)
		if e != nil {
			return nil, e
		}
		if e = validateConnect2Keys(service); e != nil {
			return nil, e
		}
		inputs = mergeSettings(env, service, explicit)
	}
	if err = validateConnect2Keys(inputs); err != nil {
		return nil, err
	}
	v := mergeSettings(defaults, inputs)
	if v["user"] == "" {
		v["user"] = osUserSettings()["user"]
	}
	if v["user"] == "" {
		return nil, errors.New("user is required")
	}
	if v["database"] == "" {
		v["database"] = v["user"]
	}
	if v["application_name"] == "" {
		v["application_name"] = v["fallback_application_name"]
	}
	for k, d := range map[string]string{"sslmode": "prefer", "sslnegotiation": "postgres", "sslsni": "1", "channel_binding": "prefer", "load_balance_hosts": "disable", "target_session_attrs": "any", "min_protocol_version": "3.0"} {
		if v[k] == "" {
			v[k] = d
		}
	}
	if v["sslrootcert"] == "system" {
		if mode, ok := inputs["sslmode"]; ok && mode != "verify-full" {
			return nil, errors.New("sslrootcert=system requires sslmode=verify-full")
		}
		v["sslmode"] = "verify-full"
	}
	for k, valid := range map[string][]string{
		"sslmode": {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, "sslnegotiation": {"postgres", "direct"}, "sslsni": {"0", "1"}, "channel_binding": {"disable", "prefer", "require"}, "target_session_attrs": {"any", "read-write", "read-only", "primary", "standby", "prefer-standby"}, "load_balance_hosts": {"disable", "random"},
	} {
		if !slices.Contains(valid, v[k]) {
			return nil, fmt.Errorf("invalid %s", k)
		}
	}
	if v["sslnegotiation"] == "direct" && slices.Contains([]string{"disable", "allow", "prefer"}, v["sslmode"]) {
		return nil, errors.New("direct TLS requires sslmode=require, verify-ca, or verify-full")
	}
	if _, err = parseRequireAuth(v["require_auth"]); err != nil {
		return nil, err
	}
	min, err := parseProtocolVersion(v["min_protocol_version"])
	if err != nil {
		return nil, err
	}
	if v["max_protocol_version"] == "" {
		v["max_protocol_version"] = "3.0"
		if min > pgproto3.ProtocolVersion30 {
			v["max_protocol_version"] = "latest"
		}
	}
	max, err := parseProtocolVersion(v["max_protocol_version"])
	if err != nil {
		return nil, err
	}
	if min > max {
		return nil, errors.New("min_protocol_version exceeds max_protocol_version")
	}
	if v["client_encoding"] == "auto" {
		return nil, errors.New("client_encoding=auto is unsupported; supply an explicit encoding")
	}
	if opts.MaxProtocolMessageBodyLen < 0 {
		return nil, errors.New("negative MaxProtocolMessageBodyLen")
	}
	s := &connectSettings{values: v, options: opts, clientOptions: make(map[string]string)}
	if t := v["connect_timeout"]; t != "" {
		n, e := strconv.ParseInt(strings.TrimSpace(t), 10, 32)
		if e != nil {
			return nil, errors.New("invalid connect_timeout")
		}
		if n > 0 {
			if n < 2 {
				n = 2
			}
			s.timeout = time.Duration(n) * time.Second
		}
	}
	for _, k := range []string{"statement_cache_capacity", "description_cache_capacity"} {
		if val, ok := v[k]; ok {
			n, e := strconv.ParseInt(val, 10, 32)
			if e != nil || n < 0 {
				return nil, fmt.Errorf("invalid %s", k)
			}
			s.clientOptions[k] = val
		}
	}
	if val, ok := v["default_query_exec_mode"]; ok {
		if !slices.Contains([]string{"cache_statement", "cache_describe", "describe_exec", "exec", "simple_protocol"}, val) {
			return nil, errors.New("invalid default_query_exec_mode")
		}
		s.clientOptions["default_query_exec_mode"] = val
	}
	names := strings.Split(v["host"], ",")
	addresses := strings.Split(v["hostaddr"], ",")
	if v["hostaddr"] != "" && inputs["host"] == "" {
		names = make([]string, len(addresses))
	}
	if v["hostaddr"] != "" && len(addresses) != len(names) {
		return nil, errors.New("host and hostaddr lists have different lengths")
	}
	ports := strings.Split(v["port"], ",")
	if len(ports) != 1 && len(ports) != len(names) {
		return nil, errors.New("host and port lists have different lengths")
	}
	for i, name := range names {
		addr := ""
		if v["hostaddr"] != "" {
			addr = addresses[i]
			if addr != "" && net.ParseIP(addr) == nil {
				return nil, errors.New("hostaddr must be a numeric IP address")
			}
		}
		if name == "" && addr == "" {
			name = defaultHost()
		}
		portText := ports[0]
		if len(ports) > 1 {
			portText = ports[i]
		}
		if portText == "" {
			portText = defaultPort
		}
		port, e := parsePort(portText)
		if e != nil {
			return nil, errors.New("invalid port")
		}
		s.hosts = append(s.hosts, logicalHost{name: name, address: addr, port: port, password: v["password"]})
	}
	// libpq resolves all password-file entries once per operation, before dialing.
	if v["password"] == "" && opts.PasswordProvider == nil {
		if fi, e := os.Stat(v["passfile"]); e == nil && fi.Mode().IsRegular() && (runtime.GOOS == "windows" || fi.Mode().Perm()&0o077 == 0) {
			if pf, e := pgpassfile.ReadPassfile(v["passfile"]); e == nil {
				for i := range s.hosts {
					h := &s.hosts[i]
					name := h.name
					if name == "" {
						name = h.address
					}
					if isAbsolutePath(name) || strings.HasPrefix(name, "@") {
						name = "localhost"
					}
					h.password = pf.FindPassword(name, strconv.Itoa(int(h.port)), v["database"], v["user"])
				}
			}
		}
	}
	return s, nil
}
