package pgconn_test

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestParseConfigSSLCertMode(t *testing.T) {
	clearPgEnvvars(t)
	t.Setenv("PGSSLCERTMODE", "")

	for _, tc := range []struct {
		connString string
		mode       string
	}{
		{"host=localhost", "allow"},
		{"host=localhost sslcertmode=allow", "allow"},
		{"host=localhost sslcertmode=disable", "disable"},
		{"host=localhost sslcertmode=require", "require"},
		{"postgres://localhost/test?sslcertmode=require", "require"},
		{"host=/tmp sslcertmode=require", "require"},
		{"host=localhost sslmode=disable sslcertmode=require", "require"},
	} {
		t.Run(tc.connString, func(t *testing.T) {
			config, err := pgconn.ParseConfig(tc.connString)
			require.NoError(t, err)
			require.Equal(t, tc.mode, config.SSLCertMode)
			require.NotContains(t, config.RuntimeParams, "sslcertmode")
			require.Equal(t, tc.mode, config.Copy().SSLCertMode)
		})
	}

	for _, connString := range []string{
		"host=localhost sslcertmode=invalid",
		"host=localhost sslmode=disable sslcertmode=invalid",
		"host=/tmp sslcertmode=invalid",
	} {
		_, err := pgconn.ParseConfig(connString)
		require.ErrorContains(t, err, "unknown sslcertmode value")
	}

	t.Setenv("PGSSLCERTMODE", "disable")
	config, err := pgconn.ParseConfig("host=localhost")
	require.NoError(t, err)
	require.Equal(t, "disable", config.SSLCertMode)
	config, err = pgconn.ParseConfig("host=localhost sslcertmode=require")
	require.NoError(t, err)
	require.Equal(t, "require", config.SSLCertMode)

	servicefile := filepath.Join(t.TempDir(), "pg_service.conf")
	require.NoError(t, os.WriteFile(servicefile, []byte("[test]\nhost=localhost\nsslcertmode=allow\n"), 0600))
	config, err = pgconn.ParseConfig(fmt.Sprintf("servicefile=%s service=test", servicefile))
	require.NoError(t, err)
	require.Equal(t, "allow", config.SSLCertMode)

	t.Setenv("PGSSLCERTMODE", "invalid")
	_, err = pgconn.ParseConfig("host=localhost")
	require.ErrorContains(t, err, "unknown sslcertmode value")
}

func TestParseConfigSSLCertModeDisableSkipsFiles(t *testing.T) {
	clearPgEnvvars(t)
	t.Setenv("PGSSLCERTMODE", "")
	dir := t.TempDir()
	certfile, keyfile := filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(certfile, []byte(rsaCertPEM), 0600))
	require.NoError(t, os.WriteFile(keyfile, []byte(rsaKeyPEM), 0600))
	config, err := pgconn.ParseConfig(fmt.Sprintf("host=localhost sslcertmode=allow sslcert=%s sslkey=%s", certfile, keyfile))
	require.NoError(t, err)
	require.Len(t, config.TLSConfig.Certificates, 1)

	// Bad files and incomplete pairs are ignored, including when there are multiple hosts.
	require.NoError(t, os.WriteFile(keyfile, []byte("invalid private key"), 0600))
	for _, files := range []string{
		fmt.Sprintf("sslcert=%s sslkey=%s", certfile, keyfile),
		"sslcert=nonexistent sslkey=nonexistent",
		"sslcert=nonexistent",
		"sslkey=nonexistent",
	} {
		config, err := pgconn.ParseConfig("host=localhost,otherhost sslcertmode=disable " + files)
		require.NoError(t, err)
		require.Empty(t, config.TLSConfig.Certificates)
		for _, fallback := range config.Fallbacks {
			if fallback.TLSConfig != nil {
				require.Empty(t, fallback.TLSConfig.Certificates)
			}
		}
	}

	t.Run("default files", func(t *testing.T) {
		skipOnWindows(t)
		home := t.TempDir()
		pgdir := filepath.Join(home, ".postgresql")
		require.NoError(t, os.Mkdir(pgdir, 0700))
		for _, file := range []string{"postgresql.crt", "postgresql.key"} {
			require.NoError(t, os.WriteFile(filepath.Join(pgdir, file), []byte("invalid"), 0600))
		}
		restore := pgconn.SetUserHomeDirLookupForTest(func() (string, error) { return home, nil })
		defer restore()
		config, err := pgconn.ParseConfig("host=localhost user=test sslmode=require sslcertmode=disable")
		require.NoError(t, err)
		require.Empty(t, config.TLSConfig.Certificates)
		_, err = pgconn.ParseConfig("host=localhost user=test sslmode=require sslcertmode=allow")
		require.ErrorContains(t, err, "failed to decode sslkey")
	})
}

type sslCertModeServerResult struct {
	certificates int
	err          error
}

// Use real TLS and PostgreSQL startup messages over pipes so these tests do not
// depend on a database, external certificates, or listening sockets.
func sslCertModeTestServer(t *testing.T, config *pgconn.Config, serverConfig *tls.Config) <-chan sslCertModeServerResult {
	t.Helper()
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
			var conn net.Conn = server
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
				result.certificates = len(tlsConn.ConnectionState().PeerCertificates)
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
			startup, ok := message.(*pgproto3.StartupMessage)
			if !ok {
				result.err = fmt.Errorf("unexpected startup message: %T", message)
				return
			}
			if _, ok := startup.Parameters["sslcertmode"]; ok {
				result.err = errors.New("sslcertmode was sent as a runtime parameter")
				return
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{1, 2, 3, 4}})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
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
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		for _, negotiation := range []string{"postgres", "direct"} {
			for _, tc := range []struct {
				mode        string
				requestCert bool
				haveCert    bool
				wantCert    bool
				wantErr     string
			}{
				{"allow", false, true, false, ""},
				{"allow", true, true, true, ""},
				{"allow", true, false, false, ""},
				{"disable", true, true, false, ""},
				{"disable", false, true, false, ""},
				{"require", true, true, true, ""},
				{"require", false, true, false, "server did not request an SSL certificate"},
				{"require", true, false, false, "server accepted connection without a valid SSL certificate"},
			} {
				name := fmt.Sprintf("%x/%s/%s/request=%v/cert=%v", version, negotiation, tc.mode, tc.requestCert, tc.haveCert)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=" + tc.mode)
					require.NoError(t, err)
					config.SSLNegotiation = negotiation
					// Supplying TLSConfig directly must still respect sslcertmode.
					config.TLSConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: version, MaxVersion: version}
					if tc.haveCert {
						config.TLSConfig.Certificates = []tls.Certificate{cert}
					}
					original := config.TLSConfig
					serverConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: version, MaxVersion: version}
					if tc.requestCert {
						serverConfig.ClientAuth = tls.RequestClientCert
					}
					results := sslCertModeTestServer(t, config, serverConfig)
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
					require.Same(t, original, config.TLSConfig)
					require.Equal(t, tc.haveCert, len(config.TLSConfig.Certificates) > 0)
					require.Nil(t, config.TLSConfig.GetClientCertificate)
				})
			}
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
		{"allow", &cert, nil, ""},
		{"disable", &cert, nil, ""},
		{"require", &cert, nil, ""},
		{"require", new(tls.Certificate), nil, "server accepted connection without a valid SSL certificate"},
		{"require", nil, callbackErr, callbackErr.Error()},
	} {
		t.Run(fmt.Sprintf("%s/%s", tc.mode, tc.wantErr), func(t *testing.T) {
			t.Parallel()
			config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=" + tc.mode)
			require.NoError(t, err)
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
	config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=disable sslcertmode=require")
	require.NoError(t, err)
	results := sslCertModeTestServer(t, config, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = pgconn.ConnectConfig(ctx, config)
	require.ErrorContains(t, err, "server did not request an SSL certificate")
	require.NoError(t, (<-results).err)
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
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		t.Error("invalid sslcertmode should be rejected before dialing")
		return nil, errors.New("unexpected dial")
	}
	_, err = pgconn.ConnectConfig(context.Background(), config)
	require.ErrorContains(t, err, "unknown sslcertmode value")
}

func TestConnectSSLCertModeCertificateSelection(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	unusable := cert
	// An RSA key cannot use this signature scheme.
	unusable.SupportedSignatureAlgorithms = []tls.SignatureScheme{tls.ECDSAWithP256AndSHA256}
	for _, haveUsable := range []bool{false, true} {
		t.Run(fmt.Sprintf("usable=%v", haveUsable), func(t *testing.T) {
			t.Parallel()
			config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=require")
			require.NoError(t, err)
			config.TLSConfig = &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{unusable}}
			if haveUsable {
				config.TLSConfig.Certificates = append(config.TLSConfig.Certificates, cert)
			}
			results := sslCertModeTestServer(t, config, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequestClientCert})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.ConnectConfig(ctx, config)
			if haveUsable {
				require.NoError(t, err)
				conn.Close(ctx)
			} else {
				require.ErrorContains(t, err, "server accepted connection without a valid SSL certificate")
			}
			result := <-results
			require.NoError(t, result.err)
			require.Equal(t, haveUsable, result.certificates > 0)
		})
	}
}

func TestCancelRequestSSLCertMode(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	for _, negotiation := range []string{"postgres", "direct"} {
		for _, mode := range []string{"allow", "disable", "require"} {
			t.Run(negotiation+"/"+mode, func(t *testing.T) {
				t.Parallel()
				config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=" + mode)
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
					require.Equal(t, mode != "disable", result.certificates > 0)
				}
			})
		}
	}
}

func TestConnectSSLCertModeSharedConfig(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	config, err := pgconn.ParseConfig("host=127.0.0.1 user=test sslmode=require sslcertmode=require")
	require.NoError(t, err)
	config.TLSConfig = &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}
	var handshakes atomic.Int32
	serverConfig := &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			c := &tls.Config{Certificates: []tls.Certificate{cert}}
			if handshakes.Add(1)%2 == 1 {
				c.ClientAuth = tls.RequestClientCert
			}
			return c, nil
		},
	}
	results := sslCertModeTestServer(t, config, serverConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connectErrors := make(chan error, 8)
	for range 8 {
		go func() {
			conn, err := pgconn.ConnectConfig(ctx, config.Copy())
			if err == nil {
				conn.Close(ctx)
			}
			connectErrors <- err
		}()
	}
	successes := 0
	for range 8 {
		if err := <-connectErrors; err == nil {
			successes++
		} else {
			require.True(t, strings.Contains(err.Error(), "server did not request an SSL certificate"), "%v", err)
		}
		require.NoError(t, (<-results).err)
	}
	require.Equal(t, 4, successes)
	require.Nil(t, config.TLSConfig.GetClientCertificate)

	// A successful certificate exchange on a rejected host must not satisfy
	// sslcertmode=require on a subsequent fallback that never requests a certificate.
	handshakes.Store(0)
	config.Fallbacks = []*pgconn.FallbackConfig{{Host: "127.0.0.1", Port: 5433, TLSConfig: config.TLSConfig}}
	config.ValidateConnect = func(context.Context, *pgconn.PgConn) error { return errors.New("try next host") }
	_, err = pgconn.ConnectConfig(ctx, config)
	require.ErrorContains(t, err, "server did not request an SSL certificate")
	for range 2 {
		require.NoError(t, (<-results).err)
	}
}

func TestConnectSSLCertModePostgreSQL(t *testing.T) {
	connString := os.Getenv("PGX_TEST_TLS_CLIENT_CONN_STRING")
	if connString == "" || os.Getenv("PGX_SSL_PASSWORD") == "" {
		t.Skip("requires PGX_TEST_TLS_CLIENT_CONN_STRING and PGX_SSL_PASSWORD")
	}
	for _, mode := range []string{"allow", "disable", "require"} {
		t.Run(mode, func(t *testing.T) {
			config, err := pgconn.ParseConfigWithOptions(connString+" sslcertmode="+mode, pgconn.ParseConfigOptions{
				GetSSLPassword: func(context.Context) string { return os.Getenv("PGX_SSL_PASSWORD") },
			})
			require.NoError(t, err)
			config.RequireAuth = "none" // Certificate authentication has no separate authentication exchange.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := pgconn.ConnectConfig(ctx, config)
			if mode == "disable" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer conn.Close(ctx)
			result := conn.ExecParams(ctx, "select client_dn is not null from pg_stat_ssl where pid = pg_backend_pid()", nil, nil, nil, nil).Read()
			require.NoError(t, result.Err)
			require.Equal(t, [][][]byte{{[]byte("t")}}, result.Rows)
		})
	}
}
