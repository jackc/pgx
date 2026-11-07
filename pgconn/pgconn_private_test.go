package pgconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		commandTag   CommandTag
		rowsAffected int64
		isInsert     bool
		isUpdate     bool
		isDelete     bool
		isSelect     bool
	}{
		{commandTag: CommandTag{s: "INSERT 0 5"}, rowsAffected: 5, isInsert: true},
		{commandTag: CommandTag{s: "UPDATE 0"}, rowsAffected: 0, isUpdate: true},
		{commandTag: CommandTag{s: "UPDATE 1"}, rowsAffected: 1, isUpdate: true},
		{commandTag: CommandTag{s: "DELETE 0"}, rowsAffected: 0, isDelete: true},
		{commandTag: CommandTag{s: "DELETE 1"}, rowsAffected: 1, isDelete: true},
		{commandTag: CommandTag{s: "DELETE 1234567890"}, rowsAffected: 1234567890, isDelete: true},
		{commandTag: CommandTag{s: "SELECT 1"}, rowsAffected: 1, isSelect: true},
		{commandTag: CommandTag{s: "SELECT 99999999999"}, rowsAffected: 99999999999, isSelect: true},
		{commandTag: CommandTag{s: "CREATE TABLE"}, rowsAffected: 0},
		{commandTag: CommandTag{s: "ALTER TABLE"}, rowsAffected: 0},
		{commandTag: CommandTag{s: "DROP TABLE"}, rowsAffected: 0},
	}

	for i, tt := range tests {
		ct := tt.commandTag
		assert.Equalf(t, tt.rowsAffected, ct.RowsAffected(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isInsert, ct.Insert(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isUpdate, ct.Update(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isDelete, ct.Delete(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isSelect, ct.Select(), "%d. %v", i, tt.commandTag)
	}
}

// timeoutNetError is a net.Error that always reports Timeout() == true.
type timeoutNetError struct{ msg string }

func (e *timeoutNetError) Error() string   { return e.msg }
func (e *timeoutNetError) Timeout() bool   { return true }
func (e *timeoutNetError) Temporary() bool { return false }

// wrappedDialError simulates a DialError wrapping a net.Error, like the error
// produced when a dial deadline is exceeded on a custom dialer.
type wrappedDialError struct {
	addr string
	err  error
}

func (e *wrappedDialError) Error() string { return fmt.Sprintf("dial %s: %s", e.addr, e.err) }
func (e *wrappedDialError) Unwrap() error { return e.err }

func TestNormalizeTimeoutError_PreservesErrorChain(t *testing.T) {
	t.Parallel()

	inner := &timeoutNetError{msg: "i/o timeout"}
	outer := &wrappedDialError{addr: "192.0.2.1:5432", err: inner}

	// Sanity check: errors.As finds the net.Error through the wrapper.
	var netErr net.Error
	require.True(t, errors.As(outer, &netErr))

	result := normalizeTimeoutError(context.Background(), outer)

	// The result should be an errTimeout and should still unwrap to the
	// original wrappedDialError so callers can inspect dial context (address, etc.).
	var te *errTimeout
	require.True(t, errors.As(result, &te))

	var dial *wrappedDialError
	assert.True(t, errors.As(result, &dial), "original dial error should be preserved in the chain")
	assert.Equal(t, "192.0.2.1:5432", dial.addr)
}

func TestBuildConnectOneConfigsCarriesPerHostPasswords(t *testing.T) {
	t.Parallel()

	config := &Config{
		Host:     "primary.example.com",
		Port:     5432,
		Password: "primary-secret",
		LookupFunc: func(ctx context.Context, host string) ([]string, error) {
			return []string{"192.0.2.1"}, nil
		},
		Fallbacks: []*FallbackConfig{
			{Host: "replica.example.com", Port: 5432, Password: "replica-secret"},
		},
	}

	configs, errs := buildConnectOneConfigs(context.Background(), config)
	require.Empty(t, errs)
	require.Len(t, configs, 2)
	assert.Equal(t, "primary.example.com", configs[0].originalHostname)
	assert.Equal(t, "primary-secret", configs[0].password)
	assert.Equal(t, "replica.example.com", configs[1].originalHostname)
	assert.Equal(t, "replica-secret", configs[1].password)
}

// TestBuildConnectOneConfigsPasswordInheritance covers the password model of #2708: a password resolved from a
// .pgpass entry for a host is authoritative for that host, even when the entry is missing, while any other host
// inherits Config.Password as it stands at connect time.
func TestBuildConnectOneConfigsPasswordInheritance(t *testing.T) {
	t.Parallel()

	resolveLocalhost := func(ctx context.Context, host string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	}

	t.Run("refreshed password applies to fallback hosts", func(t *testing.T) {
		t.Parallel()

		// Parsed with one password, then refreshed after parsing, as a BeforeConnect credential refresh
		// does.
		config, err := ParseConfig("host=primary.example.com,fallback.example.com user=app password=old-password sslmode=disable")
		require.NoError(t, err)
		config.Password = "refreshed-password"
		config.LookupFunc = resolveLocalhost

		configs, errs := buildConnectOneConfigs(context.Background(), config)
		require.Empty(t, errs)
		require.Len(t, configs, 2)
		assert.Equal(t, "refreshed-password", configs[0].password)
		assert.Equal(t, "refreshed-password", configs[1].password)
	})

	t.Run("refreshed password applies to sslmode prefer plaintext fallback", func(t *testing.T) {
		t.Parallel()

		// A single host with sslmode=prefer has a second entry for the same host with TLS disabled.
		config, err := ParseConfig("host=primary.example.com user=app password=old-password sslmode=prefer")
		require.NoError(t, err)
		config.Password = "refreshed-password"
		config.LookupFunc = resolveLocalhost

		configs, errs := buildConnectOneConfigs(context.Background(), config)
		require.Empty(t, errs)
		require.Len(t, configs, 2)
		assert.Equal(t, "primary.example.com", configs[0].originalHostname)
		assert.Equal(t, "primary.example.com", configs[1].originalHostname)
		assert.Nil(t, configs[1].tlsConfig)
		assert.Equal(t, "refreshed-password", configs[0].password)
		assert.Equal(t, "refreshed-password", configs[1].password)
	})

	t.Run("fallback without a password inherits Config.Password", func(t *testing.T) {
		t.Parallel()

		// A FallbackConfig constructed without a password, as existing callers do, must not send an empty
		// password.
		config := &Config{
			Host:       "primary.example.com",
			Port:       5432,
			Password:   "shared-password",
			LookupFunc: resolveLocalhost,
			Fallbacks: []*FallbackConfig{
				{Host: "fallback.example.com", Port: 5432},
			},
		}

		configs, errs := buildConnectOneConfigs(context.Background(), config)
		require.Empty(t, errs)
		require.Len(t, configs, 2)
		assert.Equal(t, "shared-password", configs[0].password)
		assert.Equal(t, "shared-password", configs[1].password)
	})

	t.Run("pgpass entry is kept when Config.Password is refreshed", func(t *testing.T) {
		t.Parallel()

		tfName := filepath.Join(t.TempDir(), "pgpass")
		err := os.WriteFile(tfName, []byte("db-primary:5432:app:app:primary-secret\ndb-replica:5432:app:app:replica-secret\n"), 0o600)
		require.NoError(t, err)

		config, err := ParseConfig(fmt.Sprintf("host=db-primary,db-replica dbname=app user=app sslmode=disable passfile=%s", tfName))
		require.NoError(t, err)
		config.Password = "refreshed-password"
		config.LookupFunc = resolveLocalhost

		configs, errs := buildConnectOneConfigs(context.Background(), config)
		require.Empty(t, errs)
		require.Len(t, configs, 2)
		// The primary follows Config.Password, as before this change; each fallback keeps the password its
		// own .pgpass entry resolved to.
		assert.Equal(t, "refreshed-password", configs[0].password)
		assert.Equal(t, "replica-secret", configs[1].password)
	})

	t.Run("missing pgpass entry stays empty", func(t *testing.T) {
		t.Parallel()

		tfName := filepath.Join(t.TempDir(), "pgpass")
		err := os.WriteFile(tfName, []byte("db-primary:5432:app:app:primary-secret\n"), 0o600)
		require.NoError(t, err)

		// db-replica has no entry, so it must send no password even though Config.Password holds the
		// primary host's entry.
		config, err := ParseConfig(fmt.Sprintf("host=db-primary,db-replica dbname=app user=app sslmode=disable passfile=%s", tfName))
		require.NoError(t, err)
		config.LookupFunc = resolveLocalhost

		configs, errs := buildConnectOneConfigs(context.Background(), config)
		require.Empty(t, errs)
		require.Len(t, configs, 2)
		assert.Equal(t, "primary-secret", configs[0].password)
		assert.Equal(t, "", configs[1].password)
	})
}
