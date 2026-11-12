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
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

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
