package pgconn

import (
	"context"
	"net"

	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

// connectionRuntime is shared protocol machinery's input. It contains neither
// host lists nor transport alternatives; schedulers own those decisions.
type connectionRuntime struct {
	Host                                                                                string
	Port                                                                                uint16
	User, Database, Password                                                            string
	DialFunc                                                                            DialFunc
	BuildFrontend                                                                       BuildFrontendFunc
	BuildContextWatcherHandler                                                          func(*PgConn) ctxwatch.Handler
	MaxProtocolMessageBodyLen                                                           int
	RuntimeParams                                                                       map[string]string
	KerberosSrvName, KerberosSpn                                                        string
	SSLNegotiation, MinProtocolVersion, MaxProtocolVersion, ChannelBinding, RequireAuth string
	OnNotice                                                                            NoticeHandler
	OnNotification                                                                      NotificationHandler
	OnPgError                                                                           PgErrorHandler
	OAuthTokenProvider                                                                  func(context.Context) (string, error)
	AfterNetConnect                                                                     func(context.Context, net.Conn) (net.Conn, error)
	ValidateConnect                                                                     ValidateConnectFunc
}

func legacyRuntime(c *Config) *connectionRuntime {
	r := &connectionRuntime{
		Host: c.Host, Port: c.Port, User: c.User, Database: c.Database, Password: c.Password,
		DialFunc: c.DialFunc, BuildFrontend: c.BuildFrontend, BuildContextWatcherHandler: c.BuildContextWatcherHandler,
		MaxProtocolMessageBodyLen: c.MaxProtocolMessageBodyLen, RuntimeParams: c.RuntimeParams,
		KerberosSrvName: c.KerberosSrvName, KerberosSpn: c.KerberosSpn,
		SSLNegotiation: c.SSLNegotiation, MinProtocolVersion: c.MinProtocolVersion, MaxProtocolVersion: c.MaxProtocolVersion,
		ChannelBinding: c.ChannelBinding, RequireAuth: c.RequireAuth, OnNotice: c.OnNotice,
		OnNotification: c.OnNotification, OnPgError: c.OnPgError, OAuthTokenProvider: c.OAuthTokenProvider,
		ValidateConnect: c.ValidateConnect,
	}
	if c.AfterNetConnect != nil {
		r.AfterNetConnect = func(ctx context.Context, conn net.Conn) (net.Conn, error) { return c.AfterNetConnect(ctx, c, conn) }
	}
	return r
}
