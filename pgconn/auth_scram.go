// SCRAM-SHA-256 and SCRAM-SHA-256-PLUS authentication
//
// Resources:
//   https://tools.ietf.org/html/rfc5802
//   https://tools.ietf.org/html/rfc5929
//   https://tools.ietf.org/html/rfc8265
//   https://www.postgresql.org/docs/current/sasl-authentication.html
//
// Inspiration drawn from other implementations:
//   https://github.com/lib/pq/pull/608
//   https://github.com/lib/pq/pull/788
//   https://github.com/lib/pq/pull/833

package pgconn

import (
	"bytes"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/text/secure/precis"
)

const (
	clientNonceLen      = 18
	scramSHA256Name     = "SCRAM-SHA-256"
	scramSHA256PlusName = "SCRAM-SHA-256-PLUS"
)

// Perform SCRAM authentication.
func (c *PgConn) scramAuth(serverAuthMechanisms []string) error {
	sc, err := newScramClient(serverAuthMechanisms, c.config.Password)
	if err != nil {
		return err
	}

	serverHasPlus := slices.Contains(sc.serverAuthMechanisms, scramSHA256PlusName)
	if c.config.ChannelBinding == "require" && !serverHasPlus {
		return errors.New("channel binding required but server does not support SCRAM-SHA-256-PLUS")
	}

	// If we have a TLS connection and channel binding is not disabled, attempt to
	// extract the server certificate hash for tls-server-end-point channel binding.
	if tlsConn, ok := c.conn.(*tls.Conn); ok && c.config.ChannelBinding != "disable" {
		certHash, err := getTLSCertificateHash(tlsConn)
		if err != nil && c.config.ChannelBinding == "require" {
			return fmt.Errorf("channel binding required but failed to get server certificate hash: %w", err)
		}

		// Upgrade to SCRAM-SHA-256-PLUS if we have binding data and the server supports it.
		if certHash != nil && serverHasPlus {
			sc.authMechanism = scramSHA256PlusName
		}

		sc.channelBindingData = certHash
		sc.hasTLS = true
	}

	if c.config.ChannelBinding == "require" && sc.channelBindingData == nil {
		return errors.New("channel binding required but channel binding data is not available")
	}

	// Send client-first-message in a SASLInitialResponse
	saslInitialResponse := &pgproto3.SASLInitialResponse{
		AuthMechanism: sc.authMechanism,
		Data:          sc.clientFirstMessage(),
	}
	c.frontend.Send(saslInitialResponse)
	err = c.flushWithPotentialWriteReadDeadlock()
	if err != nil {
		return err
	}

	// Receive server-first-message payload in an AuthenticationSASLContinue.
	saslContinue, err := c.rxSASLContinue()
	if err != nil {
		return err
	}
	err = sc.recvServerFirstMessage(saslContinue.Data)
	if err != nil {
		return err
	}

	if cache := c.config.ScramDeriveCache; cache != nil && sc.password != "" {
		if sp, ok := cache.Get(sc.deriveFingerprint()); ok {
			sc.saltedPassword = sp
			sc.hasSaltedPassword = true
			sc.saltedPasswordFromCache = true
		}
	}

	// Send client-final-message in a SASLResponse
	saslResponse := &pgproto3.SASLResponse{
		Data: []byte(sc.clientFinalMessage()),
	}
	c.frontend.Send(saslResponse)
	err = c.flushWithPotentialWriteReadDeadlock()
	if err != nil {
		return err
	}

	// Receive server-final-message payload in an AuthenticationSASLFinal.
	saslFinal, err := c.rxSASLFinal()
	if err != nil {
		scramInvalidateDeriveCache(c, sc)
		return err
	}
	err = sc.recvServerFinalMessage(saslFinal.Data)
	if err != nil {
		scramInvalidateDeriveCache(c, sc)
		return err
	}
	if cache := c.config.ScramDeriveCache; cache != nil && !sc.saltedPasswordFromCache && sc.hasSaltedPassword {
		cache.Put(sc.deriveFingerprint(), sc.saltedPassword)
	}
	return nil
}

func scramInvalidateDeriveCache(c *PgConn, sc *scramClient) {
	if c.config.ScramDeriveCache == nil || !sc.saltedPasswordFromCache {
		return
	}
	c.config.ScramDeriveCache.Delete(sc.deriveFingerprint())
}

func (c *PgConn) rxSASLContinue() (*pgproto3.AuthenticationSASLContinue, error) {
	msg, err := c.receiveMessage()
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case *pgproto3.AuthenticationSASLContinue:
		return m, nil
	case *pgproto3.ErrorResponse:
		return nil, ErrorResponseToPgError(m)
	}

	return nil, fmt.Errorf("expected AuthenticationSASLContinue message but received unexpected message %T", msg)
}

func (c *PgConn) rxSASLFinal() (*pgproto3.AuthenticationSASLFinal, error) {
	msg, err := c.receiveMessage()
	if err != nil {
		return nil, err
	}
	switch m := msg.(type) {
	case *pgproto3.AuthenticationSASLFinal:
		return m, nil
	case *pgproto3.ErrorResponse:
		return nil, ErrorResponseToPgError(m)
	}

	return nil, fmt.Errorf("expected AuthenticationSASLFinal message but received unexpected message %T", msg)
}

type scramClient struct {
	serverAuthMechanisms []string
	password             string
	clientNonce          []byte

	// authMechanism is the selected SASL mechanism for the client. Must be
	// either SCRAM-SHA-256 (default) or SCRAM-SHA-256-PLUS.
	//
	// Upgraded to SCRAM-SHA-256-PLUS during authentication when channel binding
	// is not disabled, channel binding data is available (TLS connection with
	// an obtainable server certificate hash) and the server advertises
	// SCRAM-SHA-256-PLUS.
	authMechanism string

	// hasTLS indicates whether the connection is using TLS. This is
	// needed because the GS2 header must distinguish between a client that
	// supports channel binding but the server does not ("y,,") versus one
	// that does not support it at all ("n,,").
	hasTLS bool

	// channelBindingData is the hash of the server's TLS certificate, computed
	// per the tls-server-end-point channel binding type (RFC 5929). Used as
	// the binding input in SCRAM-SHA-256-PLUS. nil when not in use.
	channelBindingData []byte

	clientFirstMessageBare []byte
	clientGS2Header        []byte

	serverFirstMessage   []byte
	clientAndServerNonce []byte
	salt                 []byte
	iterations           uint64

	saltedPassword          ScramSaltedPassword
	hasSaltedPassword       bool
	saltedPasswordFromCache bool
	authMessage             []byte
}

func newScramClient(serverAuthMechanisms []string, password string) (*scramClient, error) {
	sc := &scramClient{
		serverAuthMechanisms: serverAuthMechanisms,
		authMechanism:        scramSHA256Name,
	}

	// Ensure the server supports SCRAM-SHA-256. SCRAM-SHA-256-PLUS is the
	// channel binding variant and is only advertised when the server supports
	// SSL. PostgreSQL always advertises the base SCRAM-SHA-256 mechanism
	// regardless of SSL.
	if !slices.Contains(sc.serverAuthMechanisms, scramSHA256Name) {
		return nil, errors.New("server does not support SCRAM-SHA-256")
	}

	// precis.OpaqueString is equivalent to SASLprep for password.
	var err error
	sc.password, err = precis.OpaqueString.String(password)
	if err != nil {
		// PostgreSQL allows passwords invalid according to SCRAM / SASLprep.
		sc.password = password
	}

	buf := make([]byte, clientNonceLen)
	_, err = rand.Read(buf)
	if err != nil {
		return nil, err
	}
	sc.clientNonce = make([]byte, base64.RawStdEncoding.EncodedLen(len(buf)))
	base64.RawStdEncoding.Encode(sc.clientNonce, buf)

	return sc, nil
}

func (sc *scramClient) clientFirstMessage() []byte {
	// The client-first-message is the GS2 header concatenated with the bare
	// message (username + client nonce). The GS2 header communicates the
	// client's channel binding capability to the server:
	//
	//   "n,,"                      - client is not using TLS (channel binding not possible)
	//   "y,,"                      - client is using TLS but channel binding is not
	//                                in use (e.g., server did not advertise SCRAM-SHA-256-PLUS
	//                                or the server certificate hash was not obtainable)
	//   "p=tls-server-end-point,," - channel binding is active via SCRAM-SHA-256-PLUS
	//
	// See:
	//   https://www.rfc-editor.org/rfc/rfc5802#section-6
	//   https://www.rfc-editor.org/rfc/rfc5929#section-4
	//   https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256

	sc.clientFirstMessageBare = fmt.Appendf(nil, "n=,r=%s", sc.clientNonce)

	if sc.authMechanism == scramSHA256PlusName {
		sc.clientGS2Header = []byte("p=tls-server-end-point,,")
	} else if sc.hasTLS {
		sc.clientGS2Header = []byte("y,,")
	} else {
		sc.clientGS2Header = []byte("n,,")
	}

	return append(sc.clientGS2Header, sc.clientFirstMessageBare...)
}

func (sc *scramClient) recvServerFirstMessage(serverFirstMessage []byte) error {
	sc.serverFirstMessage = serverFirstMessage
	buf := serverFirstMessage
	if !bytes.HasPrefix(buf, []byte("r=")) {
		return errors.New("invalid SCRAM server-first-message received from server: did not include r=")
	}
	buf = buf[2:]

	idx := bytes.IndexByte(buf, ',')
	if idx == -1 {
		return errors.New("invalid SCRAM server-first-message received from server: did not include s=")
	}
	sc.clientAndServerNonce = buf[:idx]
	buf = buf[idx+1:]

	if !bytes.HasPrefix(buf, []byte("s=")) {
		return errors.New("invalid SCRAM server-first-message received from server: did not include s=")
	}
	buf = buf[2:]

	idx = bytes.IndexByte(buf, ',')
	if idx == -1 {
		return errors.New("invalid SCRAM server-first-message received from server: did not include i=")
	}
	saltStr := buf[:idx]
	buf = buf[idx+1:]

	if !bytes.HasPrefix(buf, []byte("i=")) {
		return errors.New("invalid SCRAM server-first-message received from server: did not include i=")
	}
	buf = buf[2:]
	iterationsStr := buf

	var err error
	sc.salt, err = base64.StdEncoding.DecodeString(string(saltStr))
	if err != nil {
		return fmt.Errorf("invalid SCRAM salt received from server: %w", err)
	}

	sc.iterations, err = strconv.ParseUint(string(iterationsStr), 10, 64)
	if err != nil || sc.iterations == 0 {
		return fmt.Errorf("invalid SCRAM iteration count received from server: %w", err)
	}

	if !bytes.HasPrefix(sc.clientAndServerNonce, sc.clientNonce) {
		return errors.New("invalid SCRAM nonce: did not start with client nonce")
	}

	if len(sc.clientAndServerNonce) <= len(sc.clientNonce) {
		return errors.New("invalid SCRAM nonce: did not include server nonce")
	}

	return nil
}

func (sc *scramClient) clientFinalMessage() string {
	// The c= attribute carries the base64-encoded channel binding input.
	//
	// Without channel binding this is just the GS2 header alone ("biws" for
	// "n,," or "eSws" for "y,,").
	//
	// With channel binding, this is the GS2 header with the channel binding data
	// (certificate hash) appended.
	channelBindInput := sc.clientGS2Header
	if sc.authMechanism == scramSHA256PlusName {
		channelBindInput = slices.Concat(sc.clientGS2Header, sc.channelBindingData)
	}
	channelBindingEncoded := base64.StdEncoding.EncodeToString(channelBindInput)
	clientFinalMessageWithoutProof := fmt.Appendf(nil, "c=%s,r=%s", channelBindingEncoded, sc.clientAndServerNonce)

	if !sc.hasSaltedPassword {
		sp, err := pbkdf2.Key(sha256.New, sc.password, sc.salt, int(sc.iterations), 32)
		if err != nil {
			panic(err) // This should never happen.
		}
		if len(sp) != 32 {
			panic("unexpected PBKDF2 output length")
		}
		copy(sc.saltedPassword[:], sp)
		sc.hasSaltedPassword = true
	}
	sc.authMessage = bytes.Join([][]byte{sc.clientFirstMessageBare, sc.serverFirstMessage, clientFinalMessageWithoutProof}, []byte(","))

	clientProof := computeClientProof(sc.saltedPassword[:], sc.authMessage)

	return fmt.Sprintf("%s,p=%s", clientFinalMessageWithoutProof, clientProof)
}

func (sc *scramClient) recvServerFinalMessage(serverFinalMessage []byte) error {
	if !bytes.HasPrefix(serverFinalMessage, []byte("v=")) {
		return errors.New("invalid SCRAM server-final-message received from server")
	}

	serverSignature := serverFinalMessage[2:]

	if !hmac.Equal(serverSignature, computeServerSignature(sc.saltedPassword[:], sc.authMessage)) {
		return errors.New("invalid SCRAM ServerSignature received from server")
	}

	return nil
}

func computeHMAC(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func computeClientProof(saltedPassword, authMessage []byte) []byte {
	clientKey := computeHMAC(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := computeHMAC(storedKey[:], authMessage)

	clientProof := make([]byte, len(clientSignature))
	for i := range clientSignature {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	buf := make([]byte, base64.StdEncoding.EncodedLen(len(clientProof)))
	base64.StdEncoding.Encode(buf, clientProof)
	return buf
}

func computeServerSignature(saltedPassword, authMessage []byte) []byte {
	serverKey := computeHMAC(saltedPassword, []byte("Server Key"))
	serverSignature := computeHMAC(serverKey, authMessage)
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(serverSignature)))
	base64.StdEncoding.Encode(buf, serverSignature)
	return buf
}

// Get the server certificate hash for SCRAM channel binding type
// tls-server-end-point.
func getTLSCertificateHash(conn *tls.Conn) ([]byte, error) {
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, errors.New("no peer certificates for channel binding")
	}

	cert := state.PeerCertificates[0]

	// Per RFC 5929 section 4.1: If the certificate's signatureAlgorithm uses
	// MD5 or SHA-1, use SHA-256. Otherwise use the hash from the signature
	// algorithm.
	//
	// See: https://www.rfc-editor.org/rfc/rfc5929.html#section-4.1
	var h hash.Hash
	switch cert.SignatureAlgorithm {
	case x509.MD5WithRSA, x509.SHA1WithRSA, x509.ECDSAWithSHA1:
		h = sha256.New()
	case x509.SHA256WithRSA, x509.SHA256WithRSAPSS, x509.ECDSAWithSHA256:
		h = sha256.New()
	case x509.SHA384WithRSA, x509.SHA384WithRSAPSS, x509.ECDSAWithSHA384:
		h = sha512.New384()
	case x509.SHA512WithRSA, x509.SHA512WithRSAPSS, x509.ECDSAWithSHA512:
		h = sha512.New()
	default:
		return nil, fmt.Errorf("tls-server-end-point channel binding is undefined for certificate signature algorithm %v", cert.SignatureAlgorithm)
	}

	h.Write(cert.Raw)
	return h.Sum(nil), nil
}
