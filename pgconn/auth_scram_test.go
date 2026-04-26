package pgconn

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func generateSelfSignedCert(t *testing.T, sigAlg x509.SignatureAlgorithm) tls.Certificate {
	t.Helper()

	var curve elliptic.Curve
	switch sigAlg {
	case x509.ECDSAWithSHA1, x509.ECDSAWithSHA256:
		curve = elliptic.P256()
	case x509.ECDSAWithSHA384:
		curve = elliptic.P384()
	case x509.ECDSAWithSHA512:
		curve = elliptic.P521()
	default:
		t.Fatalf("unsupported signature algorithm: %v", sigAlg)
	}

	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber:       big.NewInt(1),
		Subject:            pkix.Name{CommonName: "test"},
		NotBefore:          time.Now(),
		NotAfter:           time.Now().Add(time.Hour),
		SignatureAlgorithm: sigAlg,
		KeyUsage:           x509.KeyUsageDigitalSignature,
		ExtKeyUsage:        []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatal(err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
		Leaf:        cert,
	}
}

// tlsConnWithCert performs a TLS handshake over a net.Pipe using the given
// certificate and returns the client-side *tls.Conn with peer certificates
// populated.
func tlsConnWithCert(t *testing.T, cert tls.Certificate) *tls.Conn {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	t.Cleanup(func() {
		clientConn.Close()
		serverConn.Close()
	})

	tlsServer := tls.Server(serverConn, &tls.Config{
		Certificates: []tls.Certificate{cert},
	})

	tlsClient := tls.Client(clientConn, &tls.Config{
		InsecureSkipVerify: true,
	})

	errChan := make(chan error, 1)
	go func() { errChan <- tlsServer.Handshake() }()

	require.NoError(t, tlsClient.Handshake())
	require.NoError(t, <-errChan)

	return tlsClient
}

func TestGetTLSCertificateHash(t *testing.T) {
	t.Parallel()

	t.Run("SHA1", func(t *testing.T) {
		t.Parallel()

		// Per RFC 5929 section 4.1: SHA-1 signed certs use SHA-256 for the hash.
		cert := generateSelfSignedCert(t, x509.ECDSAWithSHA1)
		tlsConn := tlsConnWithCert(t, cert)

		hash, err := getTLSCertificateHash(tlsConn)
		require.NoError(t, err)
		require.Len(t, hash, sha256.Size)
	})

	t.Run("SHA256", func(t *testing.T) {
		t.Parallel()

		cert := generateSelfSignedCert(t, x509.ECDSAWithSHA256)
		tlsConn := tlsConnWithCert(t, cert)

		hash, err := getTLSCertificateHash(tlsConn)
		require.NoError(t, err)
		require.Len(t, hash, sha256.Size)
	})

	t.Run("SHA384", func(t *testing.T) {
		t.Parallel()

		cert := generateSelfSignedCert(t, x509.ECDSAWithSHA384)
		tlsConn := tlsConnWithCert(t, cert)

		hash, err := getTLSCertificateHash(tlsConn)
		require.NoError(t, err)
		require.Len(t, hash, sha512.Size384)
	})

	t.Run("SHA512", func(t *testing.T) {
		t.Parallel()

		cert := generateSelfSignedCert(t, x509.ECDSAWithSHA512)
		tlsConn := tlsConnWithCert(t, cert)

		hash, err := getTLSCertificateHash(tlsConn)
		require.NoError(t, err)
		require.Len(t, hash, sha512.Size)
	})
}

func TestScramClientFirstMessage(t *testing.T) {
	t.Parallel()

	t.Run("ChannelBindingNotSupported", func(t *testing.T) {
		t.Parallel()

		client, err := newScramClient([]string{scramSHA256Name}, "secret")
		require.NoError(t, err)

		firstMessage := client.clientFirstMessage()

		require.True(t, bytes.HasPrefix(firstMessage, []byte("n,,")))
		require.True(t, bytes.HasSuffix(firstMessage, client.clientNonce))
	})

	t.Run("ChannelBindingClientSupported", func(t *testing.T) {
		t.Parallel()

		client, err := newScramClient([]string{scramSHA256Name}, "secret")
		require.NoError(t, err)

		client.authMechanism = scramSHA256Name
		client.hasTLS = true
		client.channelBindingData = []byte{1, 2, 3}

		firstMessage := client.clientFirstMessage()
		require.True(t, bytes.HasPrefix(firstMessage, []byte("y,,")))
	})

	t.Run("ChannelBindingTLSWithoutCertHash", func(t *testing.T) {
		t.Parallel()

		// When on TLS but cert hash is unavailable (e.g., unsupported signature
		// algorithm), the client should still send "y,," to enable downgrade
		// detection per RFC 5802.
		client, err := newScramClient([]string{scramSHA256Name}, "secret")
		require.NoError(t, err)

		client.authMechanism = scramSHA256Name
		client.hasTLS = true
		client.channelBindingData = nil

		firstMessage := client.clientFirstMessage()
		require.True(t, bytes.HasPrefix(firstMessage, []byte("y,,")))
	})

	t.Run("ChannelBindingActive", func(t *testing.T) {
		t.Parallel()

		client, err := newScramClient([]string{scramSHA256Name, scramSHA256PlusName}, "secret")
		require.NoError(t, err)

		client.authMechanism = scramSHA256PlusName
		client.channelBindingData = []byte{1, 2, 3}

		firstMessage := client.clientFirstMessage()
		require.True(t, bytes.HasPrefix(firstMessage, []byte("p=tls-server-end-point,,")))
	})
}

func TestScramClientFinalMessage(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *scramClient {
		t.Helper()

		return &scramClient{
			clientNonce:   []byte("testnonce"),
			password:      "secret",
			authMechanism: scramSHA256Name,
		}
	}

	// withServerChallenge advances the scramClient through the client-first
	// and server-first (challenge) messages, leaving it ready to produce the
	// client-final-message.
	withServerChallenge := func(t *testing.T, sc *scramClient) {
		t.Helper()

		sc.clientFirstMessage()

		serverNonce := string(sc.clientNonce) + "servernonce"
		salt := base64.StdEncoding.EncodeToString([]byte("testsalt"))
		serverFirstMsg := fmt.Sprintf("r=%s,s=%s,i=4096", serverNonce, salt)
		require.NoError(t, sc.recvServerFirstMessage([]byte(serverFirstMsg)))
	}

	t.Run("ChannelBindingNone", func(t *testing.T) {
		t.Parallel()

		sc := setup(t)
		withServerChallenge(t, sc)

		msg := sc.clientFinalMessage()

		expected := base64.StdEncoding.EncodeToString([]byte("n,,"))
		require.Contains(t, msg, "c="+expected)
	})

	t.Run("ChannelBindingClientSupports", func(t *testing.T) {
		t.Parallel()

		sc := setup(t)
		sc.hasTLS = true
		sc.channelBindingData = []byte{1, 2, 3}

		withServerChallenge(t, sc)

		msg := sc.clientFinalMessage()

		expected := base64.StdEncoding.EncodeToString([]byte("y,,"))
		require.Contains(t, msg, "c="+expected)
	})

	t.Run("ChannelBindingActive", func(t *testing.T) {
		t.Parallel()

		sc := setup(t)
		sc.authMechanism = scramSHA256PlusName
		sc.hasTLS = true
		sc.channelBindingData = []byte{1, 2, 3}

		withServerChallenge(t, sc)

		msg := sc.clientFinalMessage()

		expected := base64.StdEncoding.EncodeToString(append([]byte("p=tls-server-end-point,,"), 0x01, 0x02, 0x03))
		require.Contains(t, msg, "c="+expected)
	})
}

func TestScramClientMechanismValidation(t *testing.T) {
	t.Parallel()

	// Server does not support SSL.
	_, err := newScramClient([]string{scramSHA256Name}, "password")
	require.NoError(t, err)

	// Server supports SSL.
	_, err = newScramClient([]string{scramSHA256PlusName, scramSHA256Name}, "password")
	require.NoError(t, err)

	// Invalid.
	_, err = newScramClient([]string{"MD5"}, "password")
	require.Error(t, err)
}

func TestScramClientRecvServerFirstMessage(t *testing.T) {
	t.Parallel()

	clientNonce := "testnonce"
	serverNonce := clientNonce + "servernonce"
	salt := "testsalt"
	saltEncoded := base64.StdEncoding.EncodeToString([]byte(salt))

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		// SCRAM server-first-message has the form: r=<client+server nonce>,s=<base64 salt>,i=<iterations>
		validMsg := fmt.Sprintf("r=%s,s=%s,i=%d", serverNonce, saltEncoded, 4096)

		sc := &scramClient{clientNonce: []byte(clientNonce)}
		err := sc.recvServerFirstMessage([]byte(validMsg))
		require.NoError(t, err)

		require.Equal(t, []byte(serverNonce), sc.clientAndServerNonce)
		require.Equal(t, []byte(salt), sc.salt)
		require.Equal(t, 4096, sc.iterations)
	})

	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()
		sc := &scramClient{clientNonce: []byte(clientNonce)}

		// Missing nonce.
		{
			err := sc.recvServerFirstMessage([]byte("s=" + saltEncoded + ",i=4096"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "did not include r=")
		}

		// Missing salt.
		{
			err := sc.recvServerFirstMessage([]byte("r=" + serverNonce + ",i=4096"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "did not include s=")
		}

		// Missing iterations.
		{
			err := sc.recvServerFirstMessage([]byte("r=" + serverNonce + ",s=" + saltEncoded))
			require.Error(t, err)
			require.Contains(t, err.Error(), "did not include i=")
		}

		// Invalid salt encoding.
		{
			err := sc.recvServerFirstMessage([]byte("r=" + serverNonce + ",s=%%%invalid,i=4096"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid SCRAM salt")
		}

		// Non-numeric iteration count.
		{
			err := sc.recvServerFirstMessage([]byte("r=" + serverNonce + ",s=" + saltEncoded + ",i=notanumber"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid SCRAM iteration count")
		}

		// Zero iteration count.
		{
			err := sc.recvServerFirstMessage([]byte("r=" + serverNonce + ",s=" + saltEncoded + ",i=0"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid SCRAM iteration count")
		}

		// Nonce missing client prefix.
		{
			err := sc.recvServerFirstMessage([]byte("r=wrongnonce,s=" + saltEncoded + ",i=4096"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "did not start with client nonce")
		}

		// Nonce without server contribution.
		{
			err := sc.recvServerFirstMessage([]byte("r=" + clientNonce + ",s=" + saltEncoded + ",i=4096"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "did not include server nonce")
		}
	})
}

func TestScramClientRecvServerFinalMessage(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *scramClient {
		t.Helper()

		// Build a scramClient that has completed the full message exchange up
		// through clientFinalMessage, ready to receive server-final-message.
		sc := &scramClient{
			clientNonce:   []byte("testnonce"),
			authMechanism: scramSHA256Name,
			password:      "secret",
		}
		sc.clientFirstMessage()

		serverNonce := string(sc.clientNonce) + "servernonce"
		salt := base64.StdEncoding.EncodeToString([]byte("testsalt"))
		serverFirstMsg := fmt.Sprintf("r=%s,s=%s,i=4096", serverNonce, salt)
		require.NoError(t, sc.recvServerFirstMessage([]byte(serverFirstMsg)))

		sc.clientFinalMessage()

		return sc
	}

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		sc := setup(t)

		validSignature := computeServerSignature(sc.saltedPassword[:], sc.authMessage)
		err := sc.recvServerFinalMessage(append([]byte("v="), validSignature...))
		require.NoError(t, err)
	})

	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()

		sc := setup(t)

		// Missing server signature attribute.
		{
			err := sc.recvServerFinalMessage([]byte("e=some-error"))
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid SCRAM server-final-message")
		}

		// Invalid server signature.
		{
			wrongSig := base64.StdEncoding.EncodeToString([]byte("wrong"))
			err := sc.recvServerFinalMessage([]byte("v=" + wrongSig))
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid SCRAM ServerSignature")
		}
	})
}
