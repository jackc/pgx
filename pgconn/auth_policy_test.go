package pgconn

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// connectAuthTest runs the real startup loop against a scripted server. The
// server must finish by reading either Terminate or EOF from the client.
func connectAuthTest(t *testing.T, config *Config, serve func(*pgproto3.Backend) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
	var cert tls.Certificate
	if config.TLSConfig != nil {
		cert = generateSelfSignedCert(t, x509.ECDSAWithSHA256)
	}
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		var conn net.Conn = server
		if config.TLSConfig != nil {
			backend := pgproto3.NewBackend(conn, conn)
			msg, err := backend.ReceiveStartupMessage()
			if _, ok := msg.(*pgproto3.SSLRequest); !ok {
				done <- fmt.Errorf("expected SSLRequest, got %T: %v", msg, err)
				return
			}
			if _, err := conn.Write([]byte{'S'}); err != nil {
				done <- err
				return
			}
			conn = tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}})
		}
		backend := pgproto3.NewBackend(conn, conn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			done <- err
			return
		}
		done <- serve(backend)
	}()
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	conn, err := ConnectConfig(ctx, config)
	if conn != nil {
		// The scripted server closes after Terminate without a TLS close_notify.
		_ = conn.Close(ctx)
	}
	require.NoError(t, <-done)
	return err
}

func TestConnectRequireAuthCompletion(t *testing.T) {
	for _, policy := range []string{"scram-sha-256", "password", "!none", "", "none", "scram-sha-256,none", "!password"} {
		for _, authOK := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/authOK=%v", policy, authOK), func(t *testing.T) {
				config, err := ParseConfig("host=127.0.0.1 user=test password=test sslmode=disable")
				require.NoError(t, err)
				config.RequireAuth = policy
				authRequired := policy == "scram-sha-256" || policy == "password" || policy == "!none"
				mustReject := authRequired || !authOK
				validated := false
				config.ValidateConnect = func(context.Context, *PgConn) error { validated = true; return nil }
				err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
					if authOK {
						b.Send(&pgproto3.AuthenticationOk{})
					}
					b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := b.Flush(); err != nil {
						return err
					}
					msg, err := b.Receive()
					if mustReject {
						if err == nil {
							return fmt.Errorf("expected connection closure, got %T", msg)
						}
					} else if _, ok := msg.(*pgproto3.Terminate); !ok {
						return fmt.Errorf("expected Terminate, got %T: %v", msg, err)
					}
					return nil
				})
				if mustReject {
					if authOK {
						require.ErrorContains(t, err, "server did not complete authentication")
					} else {
						require.ErrorContains(t, err, "received ReadyForQuery before AuthenticationOk")
					}
					require.False(t, validated)
				} else {
					require.NoError(t, err)
					require.True(t, validated)
				}
			})
		}
	}
}

func TestConnectAuthenticationOk(t *testing.T) {
	for _, method := range []string{"password", "md5", "oauth"} {
		for _, authOK := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/authOK=%v", method, authOK), func(t *testing.T) {
				config, err := ParseConfig("host=127.0.0.1 user=test password=test sslmode=disable require_auth=" + method)
				require.NoError(t, err)
				config.OAuthTokenProvider = func(context.Context) (string, error) { return "token", nil }
				validated := false
				config.ValidateConnect = func(context.Context, *PgConn) error { validated = true; return nil }
				err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
					switch method {
					case "password":
						b.Send(&pgproto3.AuthenticationCleartextPassword{})
					case "md5":
						b.Send(&pgproto3.AuthenticationMD5Password{})
					case "oauth":
						b.SetAuthType(pgproto3.AuthTypeSASL)
						b.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"OAUTHBEARER"}})
					}
					if err := b.Flush(); err != nil {
						return err
					}
					msg, err := b.Receive()
					if err != nil {
						return err
					}
					if method == "oauth" {
						response, ok := msg.(*pgproto3.SASLInitialResponse)
						if !ok || response.AuthMechanism != "OAUTHBEARER" {
							return fmt.Errorf("expected OAuth response, got %T", msg)
						}
					} else if _, ok := msg.(*pgproto3.PasswordMessage); !ok {
						return fmt.Errorf("expected password response, got %T", msg)
					}
					if authOK {
						b.Send(&pgproto3.AuthenticationOk{})
					}
					b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := b.Flush(); err != nil {
						return err
					}
					msg, err = b.Receive()
					if !authOK {
						if err != io.EOF && err != io.ErrUnexpectedEOF {
							return fmt.Errorf("expected EOF, got %T: %v", msg, err)
						}
					} else if _, ok := msg.(*pgproto3.Terminate); !ok {
						return fmt.Errorf("expected Terminate, got %T: %v", msg, err)
					}
					return nil
				})
				if authOK {
					require.NoError(t, err)
					require.True(t, validated)
				} else {
					if method == "oauth" {
						require.ErrorContains(t, err, "unexpected message type during OAuth auth")
					} else {
						require.ErrorContains(t, err, "received ReadyForQuery before AuthenticationOk")
					}
					require.False(t, validated)
				}
			})
		}
	}
}

func TestConnectChannelBindingRequired(t *testing.T) {
	for _, sslmode := range []string{"disable", "require"} {
		for _, tc := range []struct {
			name    string
			message pgproto3.BackendMessage
		}{
			{"ready", &pgproto3.ReadyForQuery{TxStatus: 'I'}},
			{"trust", &pgproto3.AuthenticationOk{}},
			{"password", &pgproto3.AuthenticationCleartextPassword{}},
			{"md5", &pgproto3.AuthenticationMD5Password{}},
			{"gss", &pgproto3.AuthenticationGSS{}},
			{"oauth", &pgproto3.AuthenticationSASL{AuthMechanisms: []string{"OAUTHBEARER"}}},
			{"scram", &pgproto3.AuthenticationSASL{AuthMechanisms: []string{scramSHA256Name}}},
		} {
			t.Run(sslmode+"/"+tc.name, func(t *testing.T) {
				config, err := ParseConfig("host=127.0.0.1 user=test password=test sslmode=" + sslmode + " channel_binding=require")
				require.NoError(t, err)
				tokenRequested, validated := false, false
				config.OAuthTokenProvider = func(context.Context) (string, error) { tokenRequested = true; return "secret", nil }
				config.ValidateConnect = func(context.Context, *PgConn) error { validated = true; return nil }
				err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
					b.Send(tc.message)
					if tc.name == "trust" {
						b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					}
					if err := b.Flush(); err != nil {
						return err
					}
					msg, err := b.Receive()
					if err != io.EOF && err != io.ErrUnexpectedEOF {
						return fmt.Errorf("expected EOF without credentials, got %T: %v", msg, err)
					}
					return nil
				})
				if tc.name == "ready" {
					require.ErrorContains(t, err, "received ReadyForQuery before AuthenticationOk")
				} else {
					require.ErrorContains(t, err, "channel binding required")
				}
				require.False(t, tokenRequested)
				require.False(t, validated)
			})
		}
	}
}

func TestConnectChannelBindingSCRAM(t *testing.T) {
	for _, mode := range []string{"require", "prefer", "disable"} {
		for _, tc := range []struct {
			name                         string
			invalidSignature, omitAuthOK bool
		}{
			{name: "success"}, {name: "invalidSignature", invalidSignature: true}, {name: "missingAuthOK", omitAuthOK: true},
		} {
			invalidSignature, omitAuthOK := tc.invalidSignature, tc.omitAuthOK
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				config, err := ParseConfig("host=127.0.0.1 user=test password=test sslmode=require channel_binding=" + mode + " require_auth=scram-sha-256")
				require.NoError(t, err)
				validated := false
				config.ValidateConnect = func(context.Context, *PgConn) error { validated = true; return nil }
				err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
					b.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{scramSHA256PlusName, scramSHA256Name}})
					if err := b.Flush(); err != nil {
						return err
					}
					b.SetAuthType(pgproto3.AuthTypeSASL)
					msg, err := b.Receive()
					first, ok := msg.(*pgproto3.SASLInitialResponse)
					if !ok {
						return fmt.Errorf("expected SASLInitialResponse, got %T: %v", msg, err)
					}
					mechanism := scramSHA256PlusName
					header := "p=tls-server-end-point,,"
					if mode == "disable" {
						mechanism = scramSHA256Name
						header = "n,,"
					}
					if first.AuthMechanism != mechanism || !bytes.HasPrefix(first.Data, []byte(header)) {
						return fmt.Errorf("unexpected SCRAM mechanism or header: %s %s", first.AuthMechanism, first.Data)
					}
					bare := strings.TrimPrefix(string(first.Data), header)
					_, nonce, ok := strings.Cut(bare, ",r=")
					if !ok {
						return fmt.Errorf("missing nonce")
					}
					serverFirst := "r=" + nonce + "server,s=c2FsdA==,i=4096"
					b.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte(serverFirst)})
					if err := b.Flush(); err != nil {
						return err
					}
					b.SetAuthType(pgproto3.AuthTypeSASLContinue)
					msg, err = b.Receive()
					final, ok := msg.(*pgproto3.SASLResponse)
					if !ok {
						return fmt.Errorf("expected SASLResponse, got %T: %v", msg, err)
					}
					withoutProof, _, ok := strings.Cut(string(final.Data), ",p=")
					if !ok {
						return fmt.Errorf("missing proof")
					}
					binding, _, _ := strings.Cut(strings.TrimPrefix(withoutProof, "c="), ",")
					bindingData, err := base64.StdEncoding.DecodeString(binding)
					if err != nil {
						return err
					}
					if !bytes.HasPrefix(bindingData, []byte(header)) || (mode != "disable" && len(bindingData) != len(header)+sha256.Size) {
						return fmt.Errorf("unexpected channel binding data")
					}
					salted, err := pbkdf2.Key(sha256.New, "test", []byte("salt"), 4096, 32)
					if err != nil {
						return err
					}
					signature := computeServerSignature(salted, []byte(bare+","+serverFirst+","+withoutProof))
					if invalidSignature {
						signature = []byte("invalid")
					}
					b.Send(&pgproto3.AuthenticationSASLFinal{Data: append([]byte("v="), signature...)})
					if !omitAuthOK {
						b.Send(&pgproto3.AuthenticationOk{})
					}
					b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if err := b.Flush(); err != nil {
						return err
					}
					msg, err = b.Receive()
					if invalidSignature || omitAuthOK {
						if err != io.EOF && err != io.ErrUnexpectedEOF {
							return fmt.Errorf("expected EOF, got %T: %v", msg, err)
						}
					} else if _, ok := msg.(*pgproto3.Terminate); !ok {
						return fmt.Errorf("expected Terminate, got %T: %v", msg, err)
					}
					return nil
				})
				if invalidSignature {
					require.ErrorContains(t, err, "invalid SCRAM ServerSignature")
					require.False(t, validated)
				} else if omitAuthOK {
					require.ErrorContains(t, err, "received ReadyForQuery before AuthenticationOk")
					require.False(t, validated)
				} else {
					require.NoError(t, err)
					require.True(t, validated)
				}
			})
		}
	}
}

func TestConnectOAuthServerErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		message   pgproto3.BackendMessage
		wantError string
	}{
		{"rejected", &pgproto3.AuthenticationSASLContinue{Data: []byte(`{"status":"invalid_token"}`)}, "OAuth authentication failed: invalid_token"},
		{"invalidJSON", &pgproto3.AuthenticationSASLContinue{Data: []byte(`{`)}, "invalid OAuth error response from server"},
		{"serverError", &pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000", Message: "OAuth rejected"}, "OAuth rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := ParseConfig("host=127.0.0.1 user=test sslmode=disable require_auth=oauth")
			require.NoError(t, err)
			config.OAuthTokenProvider = func(context.Context) (string, error) { return "token", nil }
			validated := false
			config.ValidateConnect = func(context.Context, *PgConn) error { validated = true; return nil }
			err = connectAuthTest(t, config, func(b *pgproto3.Backend) error {
				b.SetAuthType(pgproto3.AuthTypeSASL)
				b.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"OAUTHBEARER"}})
				if err := b.Flush(); err != nil {
					return err
				}
				msg, err := b.Receive()
				if _, ok := msg.(*pgproto3.SASLInitialResponse); !ok {
					return fmt.Errorf("expected SASLInitialResponse, got %T: %v", msg, err)
				}
				b.Send(tc.message)
				if err := b.Flush(); err != nil {
					return err
				}
				msg, err = b.Receive()
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					return fmt.Errorf("expected EOF, got %T: %v", msg, err)
				}
				return nil
			})
			require.ErrorContains(t, err, tc.wantError)
			require.False(t, validated)
		})
	}
}
