package pgconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/pgmock"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func connect2TestEnv(t *testing.T) {
	t.Helper()
	for _, e := range os.Environ() {
		key, _, _ := strings.Cut(e, "=")
		if strings.HasPrefix(key, "PG") {
			t.Setenv(key, "")
		}
	}
}

func connect2Fixture(t *testing.T, scripts ...pgmock.ConnectionScript) *pgmock.Connections {
	t.Helper()
	s := &pgmock.Connections{Scripts: scripts}
	t.Cleanup(func() { s.Close(); require.Empty(t, s.Errors) })
	return s
}

func connect2Server(messages ...pgproto3.BackendMessage) func(net.Conn) error {
	return func(c net.Conn) error {
		b := pgproto3.NewBackend(c, c)
		if _, err := b.ReceiveStartupMessage(); err != nil {
			return err
		}
		for _, m := range messages {
			b.Send(m)
		}
		if err := b.Flush(); err != nil {
			return err
		}
		// Rejected startup sockets may close without a Terminate frame. Drain
		// transport bytes instead of requiring another framed protocol message.
		_, err := io.Copy(io.Discard, c)
		if errors.Is(err, io.ErrClosedPipe) {
			return nil
		}
		return err
	}
}

func connect2OK(extra ...pgproto3.BackendMessage) func(net.Conn) error {
	msgs := []pgproto3.BackendMessage{&pgproto3.AuthenticationOk{}}
	msgs = append(msgs, extra...)
	msgs = append(msgs, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	return connect2Server(msgs...)
}

func connect2Context(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestConnect2LazyHostsAndHostaddr(t *testing.T) {
	connect2TestEnv(t)
	s := connect2Fixture(t, pgmock.ConnectionScript{Address: "192.0.2.1:5432", Run: connect2OK()})
	c, err := Connect2(connect2Context(t), "host=first,second hostaddr=192.0.2.1,192.0.2.2 user=test sslmode=disable", ConnectOptions2{DialFunc: s.Dial, LookupFunc: func(context.Context, string) ([]string, error) {
		t.Error("hostaddr performed DNS")
		return nil, errors.New("DNS")
	}})
	require.NoError(t, err)
	require.Equal(t, "first", c.ConnectionInfo().Host)
	require.Equal(t, "test", c.ConnectionInfo().Database)
	require.NoError(t, c.Close(context.Background()))
	require.Len(t, s.Calls, 1)
	s = connect2Fixture(t, pgmock.ConnectionScript{Run: connect2OK()})
	var lookups []string
	c, err = Connect2(connect2Context(t), "host=first,second user=test sslmode=disable", ConnectOptions2{DialFunc: s.Dial, LookupFunc: func(_ context.Context, host string) ([]string, error) {
		lookups = append(lookups, host)
		return []string{"192.0.2.1"}, nil
	}})
	require.NoError(t, err)
	c.Close(context.Background())
	require.Equal(t, []string{"first"}, lookups)
}

func TestConnect2PreferRefusalUsesSameSocket(t *testing.T) {
	connect2TestEnv(t)
	s := connect2Fixture(t, pgmock.ConnectionScript{Run: func(c net.Conn) error {
		b := pgproto3.NewBackend(c, c)
		m, err := b.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		if _, ok := m.(*pgproto3.SSLRequest); !ok {
			return fmt.Errorf("expected SSLRequest, got %T", m)
		}
		if _, err = c.Write([]byte{'N'}); err != nil {
			return err
		}
		return connect2OK()(c)
	}})
	c, err := Connect2(connect2Context(t), "host=192.0.2.1 user=test sslmode=prefer sslrootcert=/missing", ConnectOptions2{DialFunc: s.Dial, TLSProvider: func(context.Context, ConnectionInfo) (TLSMaterial, error) {
		t.Fatal("TLS material loaded before server accepted TLS")
		return TLSMaterial{}, nil
	}})
	require.NoError(t, err)
	require.False(t, c.ConnectionInfo().TLS)
	c.Close(context.Background())
	require.Len(t, s.Calls, 1)
}

func TestConnect2StartupTransitions(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		authOK     bool
		wantCalls  int
	}{
		{"cannot-connect", "57P03", false, 2}, {"too-many", "53300", false, 1}, {"password", "28P01", false, 1}, {"authenticated-cannot-connect", "57P03", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connect2TestEnv(t)
			var msgs []pgproto3.BackendMessage
			if tc.authOK {
				msgs = append(msgs, &pgproto3.AuthenticationOk{})
			}
			msgs = append(msgs, &pgproto3.ErrorResponse{Severity: "FATAL", Code: tc.code, Message: "fixture"})
			s := connect2Fixture(t, pgmock.ConnectionScript{Run: connect2Server(msgs...)}, pgmock.ConnectionScript{Address: "192.0.2.3:5432", Run: connect2OK()})
			c, err := Connect2(connect2Context(t), "host=first,second user=test sslmode=disable", ConnectOptions2{DialFunc: s.Dial, LookupFunc: func(_ context.Context, h string) ([]string, error) {
				if h == "first" {
					return []string{"192.0.2.1", "192.0.2.2"}, nil
				}
				return []string{"192.0.2.3"}, nil
			}})
			if tc.wantCalls == 2 {
				require.NoError(t, err)
				c.Close(context.Background())
			} else {
				var pe *PgError
				require.ErrorAs(t, err, &pe)
				require.Equal(t, tc.code, pe.Code)
			}
			require.Len(t, s.Calls, tc.wantCalls)
		})
	}
}

func TestConnect2AllowRetriesStartupErrorWithTLS(t *testing.T) {
	connect2TestEnv(t)
	s := connect2Fixture(t, pgmock.ConnectionScript{Run: connect2Server(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "fixture"})}, pgmock.ConnectionScript{Run: func(c net.Conn) error {
		b := pgproto3.NewBackend(c, c)
		msg, err := b.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		if _, ok := msg.(*pgproto3.SSLRequest); !ok {
			return fmt.Errorf("expected SSLRequest, got %T", msg)
		}
		_, err = c.Write([]byte{'N'})
		return err
	}})
	_, err := Connect2(connect2Context(t), "host=192.0.2.1 user=test sslmode=allow", ConnectOptions2{DialFunc: s.Dial})
	require.Error(t, err)
	require.Len(t, s.Calls, 2)
}

func TestConnect2TargetSkipsHostAndRelaxedPass(t *testing.T) {
	connect2TestEnv(t)
	primary := connect2OK(&pgproto3.ParameterStatus{Name: "in_hot_standby", Value: "off"})
	s := connect2Fixture(t, pgmock.ConnectionScript{Address: "192.0.2.1:5432", Run: primary}, pgmock.ConnectionScript{Address: "192.0.2.3:5432", Run: primary}, pgmock.ConnectionScript{Address: "192.0.2.1:5432", Run: primary}, pgmock.ConnectionScript{Address: "192.0.2.3:5432", Run: primary})
	var lookups, validated []string
	c, err := Connect2(connect2Context(t), "host=first,second user=test sslmode=disable target_session_attrs=prefer-standby", ConnectOptions2{DialFunc: s.Dial, LookupFunc: func(_ context.Context, h string) ([]string, error) {
		lookups = append(lookups, h)
		if h == "first" {
			return []string{"192.0.2.1", "192.0.2.2"}, nil
		}
		return []string{"192.0.2.3"}, nil
	}, ValidateConnect: func(_ context.Context, _ *PgConn, i ConnectionInfo) error {
		validated = append(validated, i.Host)
		if i.Host == "first" {
			return errors.New("reject")
		}
		return nil
	}})
	require.NoError(t, err)
	c.Close(context.Background())
	require.Equal(t, []string{"first", "second", "first", "second"}, lookups)
	require.Equal(t, []string{"first", "second"}, validated)
}

func TestConnect2RequiredChannelBindingRejectsTrustAndPassword(t *testing.T) {
	for _, m := range []pgproto3.BackendMessage{&pgproto3.AuthenticationOk{}, &pgproto3.AuthenticationCleartextPassword{}, &pgproto3.AuthenticationMD5Password{}} {
		t.Run(fmt.Sprintf("%T", m), func(t *testing.T) {
			connect2TestEnv(t)
			s := connect2Fixture(t, pgmock.ConnectionScript{Run: connect2Server(m)})
			_, err := Connect2(connect2Context(t), "host=192.0.2.1 user=test sslmode=disable channel_binding=require", ConnectOptions2{DialFunc: s.Dial})
			require.ErrorContains(t, err, "channel binding")
		})
	}
}

func TestConnect2FreshInputsAndCredentials(t *testing.T) {
	connect2TestEnv(t)
	dir := t.TempDir()
	service := filepath.Join(dir, "service")
	pass := filepath.Join(dir, "pass")
	write := func(host, password string) {
		require.NoError(t, os.WriteFile(service, []byte("[test]\nhost="+host+"\nuser=alice\nsslmode=disable\n"), 0o600))
		require.NoError(t, os.WriteFile(pass, []byte(host+":5432:alice:alice:"+password+"\n"), 0o600))
	}
	t.Setenv("PGSERVICEFILE", service)
	t.Setenv("PGSERVICE", "test")
	t.Setenv("PGPASSFILE", pass)
	for i := range 2 {
		host := fmt.Sprintf("192.0.2.%d", i+1)
		pw := fmt.Sprintf("password-%d", i)
		write(host, pw)
		s := connect2Fixture(t, pgmock.ConnectionScript{Address: net.JoinHostPort(host, "5432"), Run: func(c net.Conn) error {
			b := pgproto3.NewBackend(c, c)
			if _, err := b.ReceiveStartupMessage(); err != nil {
				return err
			}
			b.Send(&pgproto3.AuthenticationCleartextPassword{})
			if err := b.Flush(); err != nil {
				return err
			}
			m, err := b.Receive()
			if err != nil {
				return err
			}
			if p, ok := m.(*pgproto3.PasswordMessage); !ok || p.Password != pw {
				return errors.New("wrong password")
			}
			b.Send(&pgproto3.AuthenticationOk{})
			b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := b.Flush(); err != nil {
				return err
			}
			return pgmock.WaitForClose().Step(b)
		}})
		c, err := Connect2(connect2Context(t), "", ConnectOptions2{DialFunc: s.Dial})
		require.NoError(t, err)
		c.Close(context.Background())
	}
}

func TestConnect2InputValidationBeforeIO(t *testing.T) {
	connect2TestEnv(t)
	for _, input := range []string{"service=missing search_path=secret", "host=a hostaddr=192.0.2.1,192.0.2.2", "hostaddr=not-an-ip", "sslmode=invalid", "sslnegotiation=invalid", "sslnegotiation=direct sslmode=prefer", "sslrootcert=system sslmode=require", "keepalives=1", "statement_cache_capacity=-1"} {
		t.Run(input, func(t *testing.T) {
			_, err := Connect2(connect2Context(t), input, ConnectOptions2{DialFunc: func(context.Context, string, string) (net.Conn, error) {
				t.Error("dial before validation")
				return nil, errors.New("dial")
			}})
			require.Error(t, err)
		})
	}
	_, err := Connect2(connect2Context(t), "servicefile=/missing service=test", ConnectOptions2{ConnStringAllowedKeys: []string{}})
	require.ErrorContains(t, err, "ConnStringAllowedKeys")
	s, err := resolveConnectSettings("user=test connect_timeout=-1", ConnectOptions2{})
	require.NoError(t, err)
	require.Zero(t, s.timeout)
}

func TestConnect2NotificationHandoffAndHijack(t *testing.T) {
	connect2TestEnv(t)
	s := connect2Fixture(t, pgmock.ConnectionScript{Run: connect2OK(&pgproto3.NotificationResponse{PID: 42, Channel: "setup", Payload: "hello"})})
	c, err := Connect2(connect2Context(t), "host=192.0.2.1 user=test sslmode=disable", ConnectOptions2{DialFunc: s.Dial})
	require.NoError(t, err)
	require.NoError(t, c.SyncConn(context.Background()))
	hc, err := c.Hijack()
	require.NoError(t, err)
	require.Nil(t, hc.Config)
	rebuilt, err := Construct(hc)
	require.NoError(t, err)
	require.Equal(t, c.ConnectionInfo(), rebuilt.ConnectionInfo())
	var got []*Notification
	require.True(t, rebuilt.SetNotificationHandler(func(_ *PgConn, n *Notification) { got = append(got, n) }))
	require.Len(t, got, 1)
	require.Equal(t, "hello", got[0].Payload)
	rebuilt.Close(context.Background())
}

func TestConnect2SelectedSocketCancellation(t *testing.T) {
	connect2TestEnv(t)
	expected := "/second/.s.PGSQL.5432"
	s := connect2Fixture(t, pgmock.ConnectionScript{Address: "/first/.s.PGSQL.5432"}, pgmock.ConnectionScript{Address: expected, Run: connect2OK(&pgproto3.BackendKeyData{ProcessID: 123, SecretKey: []byte{1, 2, 3, 4}})}, pgmock.ConnectionScript{Address: expected, Run: func(c net.Conn) error {
		b := pgproto3.NewBackend(c, c)
		m, err := b.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		cancel, ok := m.(*pgproto3.CancelRequest)
		if !ok || cancel.ProcessID != 123 {
			return fmt.Errorf("invalid cancellation %v", m)
		}
		return nil
	}})
	ctx := connect2Context(t)
	c, err := Connect2(ctx, "host=/first,/second user=test sslmode=disable", ConnectOptions2{DialFunc: s.Dial})
	require.NoError(t, err)
	require.NoError(t, c.CancelRequest(ctx))
	c.Close(ctx)
	require.Len(t, s.Calls, 3)
}

func TestConnect2DeadlinePositionsAndTransports(t *testing.T) {
	connect2TestEnv(t)
	// Repeated equal destinations are separate address positions. TLS alternatives
	// within one position must share a deadline; timers are released before moving.
	var deadlines []time.Time
	var scripts []pgmock.ConnectionScript
	scripts = append(scripts, pgmock.ConnectionScript{}, pgmock.ConnectionScript{Run: func(c net.Conn) error {
		b := pgproto3.NewBackend(c, c)
		_, err := b.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		_, err = c.Write([]byte{'S'})
		return err
	}}, pgmock.ConnectionScript{Run: connect2OK()})
	s := connect2Fixture(t, scripts...)
	c, err := Connect2(connect2Context(t), "host=test user=test sslmode=prefer connect_timeout=2", ConnectOptions2{LookupFunc: func(context.Context, string) ([]string, error) { return []string{"192.0.2.1", "192.0.2.1"}, nil }, DialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
		d, ok := ctx.Deadline()
		require.True(t, ok)
		deadlines = append(deadlines, d)
		return s.Dial(ctx, network, address)
	}})
	require.NoError(t, err)
	c.Close(context.Background())
	require.Len(t, deadlines, 3)
	require.True(t, deadlines[1].After(deadlines[0]))
	require.Equal(t, deadlines[1], deadlines[2])
}

func TestConnect2OAuthAndProtocolValidation(t *testing.T) {
	connect2TestEnv(t)
	s := connect2Fixture(t, pgmock.ConnectionScript{Run: func(c net.Conn) error {
		b := pgproto3.NewBackend(c, c)
		if _, err := b.ReceiveStartupMessage(); err != nil {
			return err
		}
		b.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"OAUTHBEARER"}})
		if err := b.Flush(); err != nil {
			return err
		}
		b.SetAuthType(pgproto3.AuthTypeSASL)
		if _, err := b.Receive(); err != nil {
			return err
		}
		b.Send(&pgproto3.AuthenticationOk{})
		b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := b.Flush(); err != nil {
			return err
		}
		return pgmock.WaitForClose().Step(b)
	}})
	c, err := Connect2(connect2Context(t), "host=192.0.2.1 user=test sslmode=disable require_auth=oauth", ConnectOptions2{DialFunc: s.Dial, OAuthTokenProvider: func(context.Context) (string, error) { return "test-token", nil }})
	require.NoError(t, err)
	c.Close(context.Background())
	for _, messages := range [][]pgproto3.BackendMessage{
		{&pgproto3.ReadyForQuery{TxStatus: 'I'}},
		{&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 99}},
		{&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 0, UnrecognizedOptions: []string{"unexpected"}}},
		{&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 0}, &pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 0}},
	} {
		s := connect2Fixture(t, pgmock.ConnectionScript{Run: connect2Server(messages...)})
		_, err := Connect2(connect2Context(t), "host=192.0.2.1 user=test sslmode=disable max_protocol_version=3.2", ConnectOptions2{DialFunc: s.Dial})
		require.Error(t, err)
	}
}

func TestConnect2ApplicationAbortAndContext(t *testing.T) {
	connect2TestEnv(t)
	s := connect2Fixture(t, pgmock.ConnectionScript{Run: connect2OK()})
	_, err := Connect2(connect2Context(t), "host=192.0.2.1,192.0.2.2 user=test sslmode=disable", ConnectOptions2{DialFunc: s.Dial, ValidateConnect: func(context.Context, *PgConn, ConnectionInfo) error { return fmt.Errorf("policy: %w", ErrAbortConnect) }})
	require.ErrorIs(t, err, ErrAbortConnect)
	require.Len(t, s.Calls, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Connect2(ctx, "host=192.0.2.1 user=test", ConnectOptions2{DialFunc: func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("dial with cancelled context")
		return nil, nil
	}})
	require.ErrorIs(t, err, context.Canceled)
	_, err = Connect2(context.Background(), "user=''", ConnectOptions2{ConnStringAllowedKeys: []string{}})
	require.ErrorContains(t, err, "ConnStringAllowedKeys")
}

func TestConnect2DeterministicHostAndAddressBalancing(t *testing.T) {
	connect2TestEnv(t)
	var seed [32]byte
	seed[0] = 42
	s, err := resolveConnectSettings("host=a,b,c user=test sslmode=disable load_balance_hosts=random", ConnectOptions2{})
	require.NoError(t, err)
	s.random = mathrand.New(mathrand.NewChaCha8(seed))
	expectedRandom := mathrand.New(mathrand.NewChaCha8(seed))
	names := []string{"a", "b", "c"}
	expectedRandom.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	var expected, actual []string
	for _, name := range names {
		addresses := []string{name + "-1", name + "-2"}
		expectedRandom.Shuffle(len(addresses), func(i, j int) { addresses[i], addresses[j] = addresses[j], addresses[i] })
		for _, addr := range addresses {
			expected = append(expected, addr+":5432")
		}
	}
	s.options.LookupFunc = func(_ context.Context, name string) ([]string, error) { return []string{name + "-1", name + "-2"}, nil }
	s.options.DialFunc = func(_ context.Context, _, address string) (net.Conn, error) {
		actual = append(actual, address)
		return nil, errors.New("fixture")
	}
	_, err = s.connect(connect2Context(t))
	require.Error(t, err)
	require.Equal(t, expected, actual)
}

func TestConnect2LegacyFallbackEditsRemainExplicit(t *testing.T) {
	connect2TestEnv(t)
	config, err := ParseConfig("host=first,second user=test sslmode=disable")
	require.NoError(t, err)
	copied := config.Copy()
	copied.Fallbacks = nil
	var resolved []string
	copied.LookupFunc = func(_ context.Context, name string) ([]string, error) {
		resolved = append(resolved, name)
		return []string{"192.0.2.1"}, nil
	}
	calls := 0
	copied.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		calls++
		return nil, errors.New("unavailable")
	}
	_, err = ConnectConfig(connect2Context(t), copied)
	require.Error(t, err)
	require.Equal(t, []string{"first"}, resolved)
	require.Equal(t, 1, calls)
	require.Len(t, config.Fallbacks, 1)
}

func TestConnect2InputErrorsDoNotRetainSecrets(t *testing.T) {
	connect2TestEnv(t)
	for _, input := range []string{"password=super-secret port=invalid", "password='super-secret", "postgresql://user:super-secret@host:invalid/db"} {
		_, err := Connect2(context.Background(), input, ConnectOptions2{})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "super-secret")
		var legacy *ParseConfigError
		require.False(t, errors.As(err, &legacy))
	}
}

func TestConnect2LegacyAfterNetConnectFailureCleanup(t *testing.T) {
	connect2TestEnv(t)
	for _, hookErr := range []error{errors.New("wrapper failed"), nil} {
		s := connect2Fixture(t, pgmock.ConnectionScript{Run: func(c net.Conn) error {
			_, err := io.Copy(io.Discard, c)
			if errors.Is(err, io.ErrClosedPipe) {
				return nil
			}
			return err
		}})
		config, err := ParseConfig("host=192.0.2.1 user=test sslmode=disable")
		require.NoError(t, err)
		config.DialFunc = s.Dial
		var raw net.Conn
		config.AfterNetConnect = func(_ context.Context, _ *Config, c net.Conn) (net.Conn, error) { raw = c; return nil, hookErr }
		_, err = ConnectConfig(connect2Context(t), config)
		require.Error(t, err)
		if hookErr != nil {
			require.ErrorIs(t, err, hookErr)
		}
		_, err = raw.Write([]byte("closed"))
		require.Error(t, err)
	}
}
