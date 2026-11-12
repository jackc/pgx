package pgconn

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	mathrand "math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ConnectionInfo is a value snapshot of the selected destination. It contains no
// credentials and is not a configuration for reconnecting.
type ConnectionInfo struct {
	Host             string
	Port             uint16
	Network, Address string
	User, Database   string
	TLS              bool
}

// ConnectionError identifies a failed phase and destination on the Connect2
// path. Err preserves errors.Is / errors.As, including server and context errors.
type ConnectionError struct {
	Info  ConnectionInfo
	Phase string
	Err   error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connect %s %s (%s): %v", e.Info.Address, e.Info.Host, e.Phase, e.Err)
}
func (e *ConnectionError) Unwrap() error { return e.Err }

// ErrAbortConnect, wrapped or joined with an application error, stops host
// selection when returned by ValidateConnect. Other validation errors reject
// the entire logical host and continue selection.
var ErrAbortConnect = errors.New("connection selection aborted")

// TLSMaterial supplies application-managed trust roots and client certificates.
// Nil fields use the corresponding connection-string / file settings. Non-nil
// fields replace that material. Providers must return exclusively owned values.
// Verification policy, identity, SNI, and ALPN remain controlled by the string.
type TLSMaterial struct {
	RootCAs      *x509.CertPool
	Certificates []tls.Certificate
	// VerifyConnection extends (never replaces) the sslmode verification.
	VerifyConnection func(tls.ConnectionState) error
}

// ConnectionTracer observes one physical connection operation. Start's returned
// context reaches lookup, providers, dialing, and hooks. End receives the selected
// endpoint on success; errors contain phase and destination details on failure.
type ConnectionTracer interface {
	TraceConnectStart(context.Context) context.Context
	TraceConnectEnd(context.Context, ConnectionInfo, error)
}

// ConnectOptions2 supplies Go capabilities for Connect2. Zero means standard
// behavior. Values are captured on entry; callbacks must support concurrent
// calls when reused by a pool or connector, and honor the supplied context.
type ConnectOptions2 struct {
	ConnectionTracer           ConnectionTracer
	DialFunc                   DialFunc
	LookupFunc                 LookupFunc
	BuildFrontend              BuildFrontendFunc
	BuildContextWatcherHandler func(*PgConn) ctxwatch.Handler
	MaxProtocolMessageBodyLen  int
	ConnStringAllowedKeys      []string
	GetSSLPassword             GetSSLPasswordFunc
	// PasswordProvider overrides string and password-file passwords. It runs once
	// per reached logical host, with that host's effective user/database/port.
	PasswordProvider   func(context.Context, ConnectionInfo) (string, error)
	TLSProvider        func(context.Context, ConnectionInfo) (TLSMaterial, error)
	OAuthTokenProvider func(context.Context) (string, error)
	AfterNetConnect    func(context.Context, ConnectionInfo, net.Conn) (net.Conn, error)
	ValidateConnect    func(context.Context, *PgConn, ConnectionInfo) error
	AfterConnect       AfterConnectFunc
	OnNotice           NoticeHandler
	OnNotification     NotificationHandler
	OnPgError          PgErrorHandler
}

// Copy captures all mutable input fields. Callback closures and services retain
// their own synchronization obligations.
func (o ConnectOptions2) Copy() ConnectOptions2 {
	o.ConnStringAllowedKeys = slices.Clone(o.ConnStringAllowedKeys)
	return o
}

// ConnectionInfo returns an independent snapshot, including the address actually
// selected. Legacy connections also report their selected endpoint.
func (c *PgConn) ConnectionInfo() ConnectionInfo { return c.connectionInfo }

// ClientOption returns a pgx execution extension resolved by Connect2. These
// options are never sent to PostgreSQL. It exposes no mutable parsed state.
func (c *PgConn) ClientOption(name string) (string, bool) {
	v, ok := c.clientOptions[name]
	return v, ok
}

// SetNotificationHandler transfers notification delivery to handler and drains
// notifications buffered by Connect2 during startup and connection hooks. It
// must be called with exclusive ownership of c. An existing explicit handler
// takes precedence and is not replaced. Returns whether handler was installed.
func (c *PgConn) SetNotificationHandler(handler NotificationHandler) bool {
	if c.runtime.OnNotification != nil || handler == nil {
		return false
	}
	c.runtime.OnNotification = handler
	if c.config != nil {
		c.config = c.config.Copy()
		c.config.OnNotification = handler
	}
	for _, n := range c.pendingNotifications {
		handler(c, n)
	}
	c.pendingNotifications = nil
	return true
}

// Connect2 establishes a physical connection from fresh string, environment,
// service, credential, and TLS inputs. It uses logical-host / address / transport
// scheduling. No parsed configuration is retained for reuse. Legacy Connect and
// ConnectConfig retain their existing scheduler and mutable Config contract.
func Connect2(ctx context.Context, connString string, opts ConnectOptions2) (conn *PgConn, err error) {
	opts = opts.Copy()
	if t := opts.ConnectionTracer; t != nil {
		ctx = t.TraceConnectStart(ctx)
		defer func() {
			var info ConnectionInfo
			if conn != nil {
				info = conn.ConnectionInfo()
			}
			t.TraceConnectEnd(ctx, info, err)
		}()
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	s, err := resolveConnectSettings(connString, opts)
	if err != nil {
		return nil, &ConnectionError{Phase: "input", Err: err}
	}
	return s.connect(ctx)
}

func (s *connectSettings) connect(ctx context.Context) (*PgConn, error) {
	opts, v := s.options, s.values
	dial := opts.DialFunc
	if dial == nil {
		dial = makeDefaultDialer().DialContext
	}
	lookup := opts.LookupFunc
	if lookup == nil {
		lookup = makeDefaultResolver().LookupHost
	}
	rng := s.random
	if rng == nil {
		var seed [32]byte
		_, _ = rand.Read(seed[:])
		rng = mathrand.New(mathrand.NewChaCha8(seed))
	}
	hosts := slices.Clone(s.hosts)
	if v["load_balance_hosts"] == "random" {
		rng.Shuffle(len(hosts), func(i, j int) { hosts[i], hosts[j] = hosts[j], hosts[i] })
	}
	passes := 1
	if v["target_session_attrs"] == "prefer-standby" {
		passes = 2
	}
	var failures []error
	for pass := 0; pass < passes; pass++ {
	hostLoop:
		for hi := range hosts {
			h := &hosts[hi]
			if err := ctx.Err(); err != nil {
				return nil, errors.Join(append(failures, err)...)
			}
			info := ConnectionInfo{Host: h.name, Port: h.port, User: v["user"], Database: v["database"]}
			addresses := []string{h.address}
			network := "tcp"
			if h.address == "" {
				switch {
				case isAbsolutePath(h.name) || strings.HasPrefix(h.name, "@"):
					network = "unix"
					addresses = []string{h.name + "/.s.PGSQL." + strconv.Itoa(int(h.port))}
				case net.ParseIP(h.name) != nil:
					addresses = []string{h.name}
				default:
					var err error
					addresses, err = lookup(ctx, h.name)
					if err != nil {
						failures = append(failures, &ConnectionError{Info: info, Phase: "resolve", Err: err})
						continue
					}
				}
			}
			if len(addresses) == 0 {
				failures = append(failures, &ConnectionError{Info: info, Phase: "resolve", Err: errors.New("resolver returned no addresses")})
				continue
			}
			if v["load_balance_hosts"] == "random" {
				addresses = slices.Clone(addresses)
				rng.Shuffle(len(addresses), func(i, j int) { addresses[i], addresses[j] = addresses[j], addresses[i] })
			}
			password := h.password
			if opts.PasswordProvider != nil && !h.passwordProvided {
				var err error
				password, err = opts.PasswordProvider(ctx, info)
				if err != nil {
					return nil, &ConnectionError{Info: info, Phase: "credentials", Err: err}
				}
				h.password = password
				h.passwordProvided = true
			}
			for _, address := range addresses {
				if network == "tcp" {
					if _, _, e := net.SplitHostPort(address); e != nil {
						address = net.JoinHostPort(address, strconv.Itoa(int(h.port)))
					}
				}
				info.Network, info.Address = network, address
				attemptCtx := ctx
				cancel := func() {}
				if s.timeout > 0 {
					attemptCtx, cancel = context.WithTimeout(ctx, s.timeout)
				}
				transports := []bool{false}
				if network != "unix" {
					switch v["sslmode"] {
					case "allow":
						transports = []bool{false, true}
					case "prefer":
						transports = []bool{true, false}
					case "disable":
					default:
						transports = []bool{true}
					}
				}
				for ti, encrypted := range transports {
					r := s.runtime(*h, password, dial)
					c := &connectOneConfig{network: network, address: address, originalHostname: h.name, allowPlaintext: v["sslmode"] == "prefer"}

					if encrypted {
						c.tlsConfigFunc = func(ctx context.Context) (*tls.Config, error) {
							config, err := s.tlsConfig(ctx, info)
							if err == nil {
								c.tlsConfig = config
							}
							return config, err
						}
					}

					if opts.AfterNetConnect != nil {
						r.AfterNetConnect = func(ctx context.Context, conn net.Conn) (net.Conn, error) {
							i := info
							i.TLS = encrypted && !c.usedPlaintext
							return opts.AfterNetConnect(ctx, i, conn)
						}
					}
					conn, err := connectAttempt(attemptCtx, r, c, false, nil)
					if err == nil {
						conn.clientOptions = maps.Clone(s.clientOptions)
						if err = s.validateTarget(attemptCtx, conn, pass); err != nil {
							conn.Close(attemptCtx)
							cancel()
							failures = append(failures, &ConnectionError{Info: conn.connectionInfo, Phase: "target", Err: err})
							continue hostLoop
						}
						if opts.ValidateConnect != nil {
							err = opts.ValidateConnect(attemptCtx, conn, conn.connectionInfo)
							if err != nil {
								conn.Close(attemptCtx)
								cancel()
								e := &ConnectionError{Info: conn.connectionInfo, Phase: "validate", Err: err}
								if errors.Is(err, ErrAbortConnect) {
									return nil, e
								}
								failures = append(failures, e)
								continue hostLoop
							}
						}
						// AfterConnect runs only for the selected connection and aborts on error.
						if opts.AfterConnect != nil {
							err = opts.AfterConnect(attemptCtx, conn)
						}
						if err == nil {
							err = attemptCtx.Err()
						}
						if err == nil && (conn.IsClosed() || conn.IsBusy()) {
							err = errors.New("connection hook left connection closed or busy")
						}
						if err != nil {
							conn.Close(attemptCtx)
							cancel()
							return nil, &ConnectionError{Info: conn.connectionInfo, Phase: "after-connect", Err: err}
						}
						conn.runtime.Password = ""
						conn.runtime.OAuthTokenProvider = nil
						conn.runtime.RuntimeParams = nil
						conn.runtime.AfterNetConnect = nil
						cancel()
						return conn, nil
					}
					failures = append(failures, err)
					if ctx.Err() != nil {
						cancel()
						return nil, errors.Join(append(failures, ctx.Err())...)
					}
					if attemptCtx.Err() != nil {
						break
					}
					var ce *ConnectionError
					errors.As(err, &ce)
					var pe *PgError
					errors.As(err, &pe)
					if ce != nil && ce.Phase == "startup" && pe != nil && pe.Code == "57P03" {
						cancel()
						continue hostLoop
					}
					if ce != nil && (ce.Phase == "tls-handshake" || (ce.Phase == "startup" && pe != nil)) && !c.usedPlaintext && ti+1 < len(transports) {
						continue
					}
					// Dial failures advance addresses. Protocol/authentication failures stop.
					if ce != nil && ce.Phase == "dial" {
						break
					}
					cancel()
					return nil, errors.Join(failures...)
				}
				cancel()
			}
		}
	}
	return nil, errors.Join(failures...)
}

func (s *connectSettings) runtime(h logicalHost, password string, dial DialFunc) *connectionRuntime {
	o, v := s.options, s.values
	r := &connectionRuntime{
		Host:                       h.name,
		Port:                       h.port,
		User:                       v["user"],
		Database:                   v["database"],
		Password:                   password,
		DialFunc:                   dial,
		BuildFrontend:              o.BuildFrontend,
		BuildContextWatcherHandler: o.BuildContextWatcherHandler,
		MaxProtocolMessageBodyLen:  o.MaxProtocolMessageBodyLen,
		RuntimeParams:              map[string]string{},
		KerberosSrvName:            v["krbsrvname"],
		KerberosSpn:                v["krbspn"],
		SSLNegotiation:             v["sslnegotiation"],
		MinProtocolVersion:         v["min_protocol_version"],
		MaxProtocolVersion:         v["max_protocol_version"],
		ChannelBinding:             v["channel_binding"],
		RequireAuth:                v["require_auth"],
		OnNotice:                   o.OnNotice,
		OnNotification:             o.OnNotification,
		OnPgError:                  o.OnPgError,
		OAuthTokenProvider:         o.OAuthTokenProvider,
	}
	if r.Host == "" {
		r.Host = h.address
	}
	for _, k := range []string{"application_name", "client_encoding", "options", "timezone"} {
		if val := v[k]; val != "" {
			r.RuntimeParams[k] = val
		}
	}
	if r.BuildFrontend == nil {
		r.BuildFrontend = pgproto3.NewFrontend
	}
	if r.BuildContextWatcherHandler == nil {
		r.BuildContextWatcherHandler = func(c *PgConn) ctxwatch.Handler { return &DeadlineContextWatcherHandler{Conn: c.conn} }
	}
	if r.OnPgError == nil {
		r.OnPgError = func(_ *PgConn, e *PgError) bool { return !strings.EqualFold(e.Severity, "FATAL") }
	}
	return r
}

func (s *connectSettings) validateTarget(ctx context.Context, c *PgConn, pass int) error {
	target := s.values["target_session_attrs"]
	if target == "any" || target == "prefer-standby" && pass == 1 {
		return nil
	}
	standby := c.ParameterStatus("in_hot_standby")
	if target == "read-write" || target == "read-only" {
		readOnly := c.ParameterStatus("default_transaction_read_only")
		if standby == "" || readOnly == "" {
			if target == "read-write" {
				return ValidateConnectTargetSessionAttrsReadWrite(ctx, c)
			}
			return ValidateConnectTargetSessionAttrsReadOnly(ctx, c)
		}
		ro := standby == "on" || readOnly == "on"
		if target == "read-write" && ro {
			return ErrReadOnlyConnection
		}
		if target == "read-only" && !ro {
			return ErrReadWriteConnection
		}
		return nil
	}
	if standby == "" {
		if target == "primary" {
			return ValidateConnectTargetSessionAttrsPrimary(ctx, c)
		}
		return ValidateConnectTargetSessionAttrsStandby(ctx, c)
	}
	if target == "primary" && standby == "on" {
		return ErrStandbyConnection
	}
	if target != "primary" && standby != "on" {
		return ErrPrimaryConnection
	}
	return nil
}
