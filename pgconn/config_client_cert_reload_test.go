package pgconn_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// writeClientPair replaces sslcert and sslkey with a self-signed client
// certificate carrying the given serial, the way a rotation rewrites them.
func writeClientPair(t *testing.T, sslcert, sslkey string, serial int64) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "pgx-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(sslcert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(sslkey, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
}

// The client certificate pair is read on every connection, so a rotated pair
// takes effect without restarting the process. Reading it once in ParseConfig
// (the behavior this test was written against) leaves every later connection
// presenting the certificate that existed at startup.
func TestConnectReloadsClientCertificate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sslcert := filepath.Join(dir, "client.crt")
	sslkey := filepath.Join(dir, "client.key")
	writeClientPair(t, sslcert, sslkey, 1)

	config, err := pgconn.ParseConfig(fmt.Sprintf("host=localhost user=test sslmode=require sslcert='%s' sslkey='%s'", sslcert, sslkey))
	require.NoError(t, err)

	serverCert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	// Verification is not the subject here: the server only has to ask for a
	// certificate and report the one it was sent.
	serverConfig := &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequestClientCert}

	for _, want := range []int64{1, 2} {
		results := sslCertModeTestServer(t, config, serverConfig)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := pgconn.ConnectConfig(ctx, config)
		cancel()
		require.NoError(t, err)
		_ = conn.Close(context.Background()) // the pipe server has already closed its side

		result := <-results
		require.NoError(t, result.err)
		require.Equal(t, 1, result.certificates)
		require.Equal(t, want, result.peerSerial, "connection presented a stale client certificate")

		writeClientPair(t, sslcert, sslkey, want+1)
	}
}

// A pair that is unreadable at ParseConfig time is still a configuration error,
// not a surprise on the first connection.
func TestParseConfigRejectsUnreadableClientCertificate(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing")
	_, err := pgconn.ParseConfig(fmt.Sprintf("host=localhost sslmode=require sslcert='%s' sslkey='%s'", missing, missing))
	require.ErrorContains(t, err, "unable to read sslkey")
}

// writeCombinedPair writes the key and the certificate into one file, in the
// order given, the way an operator who keeps a single PEM per identity does.
func writeCombinedPair(t *testing.T, path string, serial int64, keyFirst bool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "pgx-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if keyFirst {
		require.NoError(t, os.WriteFile(path, append(keyPEM, certPEM...), 0o600))
	} else {
		require.NoError(t, os.WriteFile(path, append(certPEM, keyPEM...), 0o600))
	}
}

// sslcert and sslkey may name the same file, and the key may sit after the
// certificate in it: X509KeyPair skips the blocks that are not a key.
func TestConnectCombinedKeyAndCertFile(t *testing.T) {
	t.Parallel()

	serverCert, err := tls.X509KeyPair([]byte(rsaCertPEM), []byte(rsaKeyPEM))
	require.NoError(t, err)
	serverConfig := &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequestClientCert}

	for _, keyFirst := range []bool{true, false} {
		name := "key first"
		if !keyFirst {
			name = "key last"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "client.pem")
			writeCombinedPair(t, path, 7, keyFirst)

			config, err := pgconn.ParseConfig(fmt.Sprintf("host=localhost user=test sslmode=require sslcert='%s' sslkey='%s'", path, path))
			require.NoError(t, err)

			results := sslCertModeTestServer(t, config, serverConfig)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			conn, err := pgconn.ConnectConfig(ctx, config)
			cancel()
			require.NoError(t, err)
			_ = conn.Close(context.Background())

			result := <-results
			require.NoError(t, result.err)
			require.Equal(t, int64(7), result.peerSerial)
		})
	}
}
