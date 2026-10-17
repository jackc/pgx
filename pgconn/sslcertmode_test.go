package pgconn_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/internal/bgreader"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestParseConfigSSLCertMode(t *testing.T) {
	clearPgEnvvars(t)
	t.Setenv("PGSSLCERTMODE", "")
	require.NoError(t, os.Unsetenv("PGSSLCERTMODE"))
	config, err := pgconn.ParseConfig("host=localhost")
	require.NoError(t, err)
	require.Equal(t, "allow", config.SSLCertMode)

	servicefile := filepath.Join(t.TempDir(), "pg_service.conf")
	require.NoError(t, os.WriteFile(servicefile, []byte("[test]\nhost=localhost\nsslcertmode=allow\n[empty]\nhost=localhost\nsslcertmode=\n"), 0o600))
	service := fmt.Sprintf("servicefile='%s'", filepath.ToSlash(servicefile))
	t.Setenv("PGSSLCERTMODE", "require")
	for _, tc := range []struct {
		connString string
		wantMode   string // Empty means an invalid connection parameter.
	}{
		{"host=localhost", "require"},
		{"host=localhost sslcertmode=allow", "allow"},
		{"host=localhost sslcertmode=disable", "disable"},
		{"postgres://localhost/test?sslcertmode=require", "require"},
		{"host=localhost sslmode=disable sslcertmode=invalid", ""},
		{"host=localhost sslcertmode=''", ""},
		{"postgres://localhost/test?sslcertmode=", ""},
		{service + " service=test", "allow"},
		{service + " service=empty", ""},
		{service + " service=empty sslcertmode=require", "require"},
	} {
		t.Run(tc.connString, func(t *testing.T) {
			config, err := pgconn.ParseConfig(tc.connString)
			if tc.wantMode == "" {
				var parseErr *pgconn.ParseConfigError
				require.ErrorAs(t, err, &parseErr)
				require.ErrorContains(t, err, "invalid sslcertmode")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantMode, config.SSLCertMode)
				require.NotContains(t, config.RuntimeParams, "sslcertmode")
			}
		})
	}

	t.Setenv("PGSSLCERTMODE", "")
	_, err = pgconn.ParseConfig("host=localhost")
	require.ErrorContains(t, err, "invalid sslcertmode")
	_, err = pgconn.ParseConfig("host=localhost sslcertmode=allow")
	require.NoError(t, err)
}

func TestParseConfigSSLCertModeDisableSkipsFiles(t *testing.T) {
	missing := filepath.ToSlash(filepath.Join(t.TempDir(), "missing"))
	config, err := pgconn.ParseConfig(fmt.Sprintf("host=localhost sslmode=require sslcertmode=disable sslcert='%s' sslkey='%s'", missing, missing))
	require.NoError(t, err)
	require.Empty(t, config.TLSConfig.Certificates)
}

type sslCertModeServerResult struct {
	resumed      bool
	certificates int
	err          error
}

type sslCertModeServerConn struct {
	net.Conn
	reader io.Reader
}

func (c sslCertModeServerConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// Use real TLS and PostgreSQL startup messages over pipes so these tests do not
// depend on a database, external certificates, or listening sockets.
func sslCertModeTestServer(t *testing.T, config *pgconn.Config, serverConfig *tls.Config, startupMessages ...pgproto3.BackendMessage) <-chan sslCertModeServerResult {
	t.Helper()
	if len(startupMessages) == 0 {
		startupMessages = []pgproto3.BackendMessage{
			&pgproto3.AuthenticationOk{},
			&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{1, 2, 3, 4}},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		}
	}
	results := make(chan sslCertModeServerResult, 32)
	direct := config.SSLNegotiation == "direct"
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		deadline := time.Now().Add(5 * time.Second)
		client.SetDeadline(deadline)
		server.SetDeadline(deadline)
		t.Cleanup(func() { client.Close(); server.Close() })
		go func() {
			defer server.Close()
			result := sslCertModeServerResult{}
			defer func() { results <- result }()
			// Both peers may write at once during TLS 1.3 session resumption.
			reader := bgreader.New(server)
			reader.Start()
			var conn net.Conn = sslCertModeServerConn{Conn: server, reader: reader}
			if serverConfig != nil {
				if !direct {
					request := make([]byte, 8)
					if _, result.err = io.ReadFull(conn, request); result.err != nil {
						return
					}
					if binary.BigEndian.Uint32(request[:4]) != 8 || binary.BigEndian.Uint32(request[4:]) != 80877103 {
						result.err = fmt.Errorf("unexpected SSLRequest: %x", request)
						return
					}
					if _, result.err = conn.Write([]byte("S")); result.err != nil {
						return
					}
				}
				tlsConn := tls.Server(conn, serverConfig)
				if result.err = tlsConn.Handshake(); result.err != nil {
					return
				}
				state := tlsConn.ConnectionState()
				result.certificates = len(state.PeerCertificates)
				result.resumed = state.DidResume
				conn = tlsConn
			}
			backend := pgproto3.NewBackend(conn, conn)
			message, err := backend.ReceiveStartupMessage()
			if err != nil {
				result.err = err
				return
			}
			if _, ok := message.(*pgproto3.CancelRequest); ok {
				return
			}
			_, ok := message.(*pgproto3.StartupMessage)
			if !ok {
				result.err = fmt.Errorf("unexpected startup message: %T", message)
				return
			}
			for _, msg := range startupMessages {
				backend.Send(msg)
			}
			result.err = backend.Flush()
		}()
		return client, nil
	}
	return results
}

func TestConnectSSLCertMode(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	unusable := cert
	unusable.SupportedSignatureAlgorithms = []tls.SignatureScheme{tls.ECDSAWithP256AndSHA256}
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		for _, tc := range []struct {
			mode        string
			requestCert bool
			haveCert    bool
			wantCert    bool
			wantErr     string
		}{
			{"", false, false, false, ""}, // An empty Config field means allow.
			{"allow", true, true, true, ""},
			{"allow", true, false, false, ""},
			{"disable", true, true, false, ""},
			{"require", true, true, true, ""},
			{"require", false, true, false, "server did not request an SSL certificate"},
			{"require", true, false, false, "server accepted connection without a valid SSL certificate"},
		} {
			t.Run(fmt.Sprintf("%x/%s/request=%v/cert=%v", version, tc.mode, tc.requestCert, tc.haveCert), func(t *testing.T) {
				t.Parallel()
				config, err := pgconn.ParseConfig("host=127.0.0.1,127.0.0.2 user=test sslmode=prefer sslcertmode=allow")
				require.NoError(t, err)
				config.SSLCertMode = tc.mode
				config.TLSConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version, Certificates: []tls.Certificate{unusable}}
				if tc.haveCert {
					config.TLSConfig.Certificates = append(config.TLSConfig.Certificates, cert)
				}
				serverConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: version, MaxVersion: version}
				if tc.requestCert {
					serverConfig.ClientAuth = tls.RequestClientCert
				}
				results := sslCertModeTestServer(t, config, serverConfig)
				dial := config.DialFunc
				attempts := 0
				config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
					attempts++
					if attempts > 1 {
						return nil, errors.New("unexpected TLS or host fallback")
					}
					return dial(ctx, network, address)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				conn, err := pgconn.ConnectConfig(ctx, config)
				if tc.wantErr == "" {
					require.NoError(t, err)
					conn.Close(ctx)
				} else {
					require.ErrorContains(t, err, tc.wantErr)
				}
				result := <-results
				require.NoError(t, result.err)
				require.Equal(t, tc.wantCert, result.certificates > 0)
				require.Equal(t, 1, attempts)
				require.Nil(t, config.TLSConfig.GetClientCertificate)
				require.NotEmpty(t, config.TLSConfig.Certificates)
			})
		}
	}
}

func TestConnectSSLCertModeCallback(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	callbackErr := errors.New("certificate callback failed")
	for _, tc := range []struct {
		mode    string
		cert    *tls.Certificate
		err     error
		wantErr string
	}{
		{"disable", &cert, nil, ""},
		{"require", &cert, nil, ""},
		{"require", new(tls.Certificate), nil, "server accepted connection without a valid SSL certificate"},
		{"require", nil, callbackErr, callbackErr.Error()},
	} {
		t.Run(fmt.Sprintf("%s/%s", tc.mode, tc.wantErr), func(t *testing.T) {
			t.Parallel()
			config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=" + tc.mode)
			require.NoError(t, err)
			config.SSLNegotiation = "direct"
			var called bool
			config.TLSConfig = &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{cert},
				GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
					called = true
					return tc.cert, tc.err
				},
			}
			results := sslCertModeTestServer(t, config, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequestClientCert})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.ConnectConfig(ctx, config)
			if tc.wantErr == "" {
				require.NoError(t, err)
				conn.Close(ctx)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
				}
			}
			result := <-results
			if tc.err == nil {
				require.NoError(t, result.err)
				require.Equal(t, tc.mode != "disable" && len(tc.cert.Certificate) > 0, result.certificates > 0)
			}
			require.Equal(t, tc.mode != "disable", called)
			require.NotNil(t, config.TLSConfig.GetClientCertificate)
		})
	}
}

func TestConnectSSLCertModeRequireWithoutTLS(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		sslmode              string
		skipAuthenticationOk bool
	}{
		{"allow", false},
		{"allow", true},
		{"disable", true},
	} {
		t.Run(fmt.Sprintf("%s/skipAuthenticationOk=%v", tc.sslmode, tc.skipAuthenticationOk), func(t *testing.T) {
			config, err := pgconn.ParseConfig("host=127.0.0.1,127.0.0.2 user=test sslcertmode=require sslmode=" + tc.sslmode)
			require.NoError(t, err)
			var startupMessages []pgproto3.BackendMessage
			if tc.skipAuthenticationOk {
				startupMessages = []pgproto3.BackendMessage{&pgproto3.ReadyForQuery{TxStatus: 'I'}}
			}
			results := sslCertModeTestServer(t, config, nil, startupMessages...)
			config.ValidateConnect = func(context.Context, *pgconn.PgConn) error {
				t.Error("certificate requirement must be checked before ValidateConnect")
				return nil
			}
			dial := config.DialFunc
			attempts := 0
			config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				attempts++
				if attempts > 1 {
					return nil, errors.New("unexpected TLS or host fallback")
				}
				return dial(ctx, network, address)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = pgconn.ConnectConfig(ctx, config)
			require.ErrorContains(t, err, "server did not request an SSL certificate")
			require.Equal(t, 1, attempts)
			require.NoError(t, (<-results).err)
		})
	}
}

func TestConnectSSLCertModeInvalid(t *testing.T) {
	t.Parallel()
	config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=disable")
	require.NoError(t, err)
	config.SSLCertMode = "invalid"
	config.LookupFunc = func(context.Context, string) ([]string, error) {
		t.Error("invalid sslcertmode should be rejected before resolving hostnames")
		return nil, errors.New("unexpected lookup")
	}
	_, err = pgconn.ConnectConfig(context.Background(), config)
	require.ErrorContains(t, err, "unknown sslcertmode value")
}

func TestConnectSSLCertModeRequireAllowsDialFallback(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	config, err := pgconn.ParseConfig("host=127.0.0.1,127.0.0.2 user=test sslmode=require sslcertmode=require")
	require.NoError(t, err)
	config.TLSConfig = &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}
	config.Fallbacks[0].TLSConfig = config.TLSConfig
	results := sslCertModeTestServer(t, config, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequestClientCert})
	dial := config.DialFunc
	var attempts atomic.Int32
	config.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("first host unavailable")
		}
		return dial(ctx, network, address)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)
	require.NoError(t, (<-results).err)
	require.EqualValues(t, 2, attempts.Load())
}

func TestConnectSSLCertModeSessionResumption(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	// Session caches reject expired certificates even with InsecureSkipVerify.
	clock := func() time.Time { return leaf.NotBefore.Add(time.Hour) }
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		for _, mode := range []string{"allow", "require"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				t.Parallel()
				config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=" + mode)
				require.NoError(t, err)
				cache := tls.NewLRUClientSessionCache(1)
				config.TLSConfig = &tls.Config{
					InsecureSkipVerify: true,
					Certificates:       []tls.Certificate{cert},
					MinVersion:         version,
					MaxVersion:         version,
					ClientSessionCache: cache,
					Time:               clock,
				}
				serverConfig := &tls.Config{
					Certificates: []tls.Certificate{cert},
					MinVersion:   version,
					MaxVersion:   version,
					ClientAuth:   tls.RequestClientCert,
					Time:         clock,
				}
				results := sslCertModeTestServer(t, config, serverConfig)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for attempt := range 2 {
					conn, err := pgconn.ConnectConfig(ctx, config)
					require.NoError(t, err)
					conn.Close(ctx)
					result := <-results
					require.NoError(t, result.err)
					require.Equal(t, attempt == 1 && mode == "allow", result.resumed)
					require.NotZero(t, result.certificates)
				}
				require.Same(t, cache, config.TLSConfig.ClientSessionCache)
			})
		}
	}
}

func TestCancelRequestSSLCertModeDisable(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	for _, negotiation := range []string{"postgres", "direct"} {
		t.Run(negotiation, func(t *testing.T) {
			t.Parallel()
			config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=disable")
			require.NoError(t, err)
			config.SSLNegotiation = negotiation
			config.TLSConfig = &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}
			results := sslCertModeTestServer(t, config, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequestClientCert})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.ConnectConfig(ctx, config)
			require.NoError(t, err)
			defer conn.Close(ctx)
			require.NoError(t, conn.CancelRequest(ctx))
			for range 2 {
				result := <-results
				require.NoError(t, result.err)
				require.Zero(t, result.certificates)
			}
		})
	}
}

func TestConnectSSLCertModeFallbackState(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=require")
	require.NoError(t, err)
	config.TLSConfig = &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}
	config.Fallbacks = []*pgconn.FallbackConfig{{Host: "127.0.0.2", Port: config.Port, TLSConfig: config.TLSConfig}}
	config.ValidateConnect = func(context.Context, *pgconn.PgConn) error { return errors.New("try next host") }
	var handshakes atomic.Int32
	results := sslCertModeTestServer(t, config, &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			c := &tls.Config{Certificates: []tls.Certificate{cert}}
			if handshakes.Add(1) == 1 {
				c.ClientAuth = tls.RequestClientCert
			}
			return c, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The first host's certificate exchange must not satisfy the second host's check.
	_, err = pgconn.ConnectConfig(ctx, config)
	require.ErrorContains(t, err, "server did not request an SSL certificate")
	for range 2 {
		require.NoError(t, (<-results).err)
	}
}
