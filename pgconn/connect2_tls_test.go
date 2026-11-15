package pgconn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/pgmock"
	"github.com/stretchr/testify/require"
)

func connect2Certificate(t *testing.T, name string) ([]byte, []byte, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk}), cert
}

func TestConnect2TLSRefreshIdentityAndProvider(t *testing.T) {
	connect2TestEnv(t)
	ctx := context.Background()
	dir := t.TempDir()
	root := filepath.Join(dir, "root.pem")
	crt := filepath.Join(dir, "client.pem")
	key := filepath.Join(dir, "client.key")
	var old *tls.Config
	for i := range 2 {
		certPEM, keyPEM, cert := connect2Certificate(t, "db.example")
		require.NoError(t, os.WriteFile(root, certPEM, 0o600))
		require.NoError(t, os.WriteFile(crt, certPEM, 0o600))
		require.NoError(t, os.WriteFile(key, keyPEM, 0o600))
		input := "host=db.example hostaddr=192.0.2.1 user=test sslmode=verify-full sslsni=0 sslrootcert='" + root + "' sslcert='" + crt + "' sslkey='" + key + "'"
		s, err := resolveConnectSettings(input, ConnectOptions2{})
		require.NoError(t, err)
		config, err := s.tlsConfig(ctx, ConnectionInfo{Host: "db.example", Address: net.JoinHostPort("192.0.2.1", "5432")})
		require.NoError(t, err)
		require.Empty(t, config.ServerName)
		require.NoError(t, config.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}))
		if i == 1 {
			require.Error(t, old.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}))
			require.NotEqual(t, old.Certificates[0].Certificate, config.Certificates[0].Certificate)
		}
		old = config
		// Explicit material overrides file sources, but cannot override policy.
		pool := x509.NewCertPool()
		pool.AddCert(cert)
		s, err = resolveConnectSettings("host=wrong.example user=test sslmode=verify-full sslrootcert=/missing", ConnectOptions2{TLSProvider: func(context.Context, ConnectionInfo) (TLSMaterial, error) { return TLSMaterial{RootCAs: pool}, nil }})
		require.NoError(t, err)
		config, err = s.tlsConfig(ctx, ConnectionInfo{Host: "wrong.example"})
		require.NoError(t, err)
		require.Error(t, config.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}))
	}
}

func TestConnectionInfoTLSState(t *testing.T) {
	connect2TestEnv(t)
	certPEM, keyPEM, cert := connect2Certificate(t, "db.example")
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	for _, path := range []string{"new", "legacy", "rejected"} {
		t.Run(path, func(t *testing.T) {
			ctx := connect2Context(t)
			s := connect2Fixture(t, pgmock.ConnectionScript{Run: func(c net.Conn) error {
				server := tls.Server(c, &tls.Config{
					Certificates: []tls.Certificate{certificate}, NextProtos: []string{"postgresql"},
					MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
				})
				if err := server.HandshakeContext(ctx); err != nil {
					return err
				}
				return connect2OK()(server)
			}})
			wrap := func(c net.Conn) net.Conn { return struct{ net.Conn }{c} }
			check := func(info ConnectionInfo) {
				require.NotNil(t, info.TLS)
				require.True(t, info.TLS.HandshakeComplete)
				require.Equal(t, uint16(tls.VersionTLS13), info.TLS.Version)
				require.NotZero(t, info.TLS.CipherSuite)
				require.Equal(t, "postgresql", info.TLS.NegotiatedProtocol)
				require.Len(t, info.TLS.PeerCertificates, 1)
				require.Equal(t, cert.Raw, info.TLS.PeerCertificates[0].Raw)
			}
			var c *PgConn
			var err error
			sentinel := errors.New("reject TLS connection")
			if path == "legacy" {
				config, parseErr := ParseConfig("host=192.0.2.1 user=test sslmode=disable")
				require.NoError(t, parseErr)
				config.SSLNegotiation = "direct"
				config.TLSConfig = &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"postgresql"}}
				config.DialFunc = s.Dial
				config.AfterNetConnect = func(_ context.Context, _ *Config, conn net.Conn) (net.Conn, error) { return wrap(conn), nil }
				c, err = ConnectConfig(ctx, config)
			} else {
				c, err = Connect2(ctx, "host=db.example hostaddr=192.0.2.1 user=test sslmode=require sslnegotiation=direct", ConnectOptions2{
					DialFunc: s.Dial,
					TLSProvider: func(_ context.Context, info ConnectionInfo) (TLSMaterial, error) {
						require.Nil(t, info.TLS)
						return TLSMaterial{}, nil
					},
					AfterNetConnect: func(_ context.Context, info ConnectionInfo, conn net.Conn) (net.Conn, error) {
						check(info)
						info.TLS.Version = 0
						return wrap(conn), nil
					},
					ValidateConnect: func(_ context.Context, _ *PgConn, info ConnectionInfo) error {
						check(info)
						info.TLS.Version = 0
						if path == "rejected" {
							return sentinel
						}
						return nil
					},
				})
			}
			if path == "rejected" {
				require.ErrorIs(t, err, sentinel)
				var ce *ConnectionError
				require.ErrorAs(t, err, &ce)
				check(ce.Info)
				return
			}
			require.NoError(t, err)
			defer c.Close(ctx)
			check(c.ConnectionInfo())
			info := c.ConnectionInfo()
			info.TLS.Version = 0
			check(c.ConnectionInfo())
			require.NoError(t, c.SyncConn(ctx))
			hijacked, err := c.Hijack()
			require.NoError(t, err)
			rebuilt, err := Construct(hijacked)
			require.NoError(t, err)
			defer rebuilt.Close(ctx)
			check(rebuilt.ConnectionInfo())
		})
	}
}
