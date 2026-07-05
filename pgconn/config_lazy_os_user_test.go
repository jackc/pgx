package pgconn_test

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseConfigDoesNotLookUpOSUserWhenUserAndPassfileExplicit reproduces
// https://github.com/jackc/pgx/issues/2586: ParseConfigWithOptions must not
// need to resolve the OS user account when the connection string already
// supplies both user and passfile. Looking up the OS account can crash the
// process in some restricted/broken-NSS container environments (SIGFPE),
// so the lookup is simulated as fatal here to prove it is never reached.
func TestParseConfigDoesNotLookUpOSUserWhenUserAndPassfileExplicit(t *testing.T) {
	skipOnWindows(t)
	clearPgEnvvars(t)
	t.Setenv("PGPASSFILE", "")
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")

	restore := pgconn.SetOSUserLookupForTest(func() (*user.User, error) {
		panic("os/user.Current must not be called when user and passfile are both explicit")
	})
	defer restore()

	passfile := filepath.Join(t.TempDir(), "pgpass")
	require.NoError(t, os.WriteFile(passfile, []byte("test1:5432:curlydb:curly:nyuknyuknyuk"), 0o600))

	connString := fmt.Sprintf("postgres://curly@test1:5432/curlydb?sslmode=disable&passfile=%s", passfile)

	// Must not panic: the mocked lookup above panics if it is ever invoked.
	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	assert.Equal(t, "curly", config.User)
	// Password must come from the explicit passfile, not from any OS-derived
	// (leaked) default path.
	assert.Equal(t, "nyuknyuknyuk", config.Password)
}

// TestParseConfigDoesNotLookUpOSUserWhenUserAndPasswordExplicit covers the
// concrete distroless/production scenario from issue #2586: user and
// password supplied directly (no reliance on a pgpass file at all).
func TestParseConfigDoesNotLookUpOSUserWhenUserAndPasswordExplicit(t *testing.T) {
	skipOnWindows(t)
	clearPgEnvvars(t)
	t.Setenv("PGPASSFILE", "")
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")

	restore := pgconn.SetOSUserLookupForTest(func() (*user.User, error) {
		panic("os/user.Current must not be called when user and password are both explicit")
	})
	defer restore()

	connString := "postgres://curly:nyuknyuknyuk@test1:5432/curlydb?sslmode=disable"

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	assert.Equal(t, "curly", config.User)
	assert.Equal(t, "nyuknyuknyuk", config.Password)
}

// TestParseConfigStillLooksUpOSUserWhenNeeded is the negative control for
// the two tests above: when the connection string does NOT supply enough
// information to avoid it, ParseConfigWithOptions must still fall back to
// the OS user account, exactly as before this change. This proves the
// laziness is conditional, not a blanket skip.
func TestParseConfigStillLooksUpOSUserWhenNeeded(t *testing.T) {
	skipOnWindows(t)
	clearPgEnvvars(t)
	t.Setenv("PGPASSFILE", "")
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")

	called := false
	restore := pgconn.SetOSUserLookupForTest(func() (*user.User, error) {
		called = true
		return &user.User{Username: "mocked-os-user", HomeDir: t.TempDir()}, nil
	})
	defer restore()

	connString := "postgres://test1:5432/curlydb?sslmode=disable"

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	assert.True(t, called, "expected the OS user lookup to be consulted for the default user")
	assert.Equal(t, "mocked-os-user", config.User)
}

// TestParseConfigOSUserLookupFailureStillErrorsWhenActuallyNeeded documents
// that when the OS lookup genuinely fails and no fallback is provided any
// other way, ParseConfig still surfaces the same "no default" behavior as
// before (empty user), rather than panicking or silently misbehaving.
func TestParseConfigOSUserLookupFailureStillErrorsWhenActuallyNeeded(t *testing.T) {
	skipOnWindows(t)
	clearPgEnvvars(t)
	t.Setenv("PGPASSFILE", "")
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")

	restore := pgconn.SetOSUserLookupForTest(func() (*user.User, error) {
		return nil, errors.New("boom")
	})
	defer restore()

	connString := "postgres://test1:5432/curlydb?sslmode=disable"

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)
	assert.Equal(t, "", config.User)
}
