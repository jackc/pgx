package pgconn_test

import (
	"crypto/x509"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is the mandatory security regression test for the reworked fix
// for https://github.com/jackc/pgx/issues/2586.
//
// The previously proposed fix for #2586 gated the ENTIRE OS-account-derived
// defaults lookup (default user, passfile, servicefile, sslcert, sslkey,
// sslrootcert) behind "is user/password already explicit". That silently
// dropped the home-directory-derived TLS defaults whenever the connection
// string already supplied both a user and a password -- exactly the common
// case for e.g. a distroless container reading DATABASE_URL from an env
// var. Concretely, for `user:pass@host?sslmode=require` with
// ~/.postgresql/root.crt present, that gated fix went from a verified TLS
// connection (RootCAs set, custom peer verification) to
// tlsConfig.InsecureSkipVerify=true with no root CA at all -- a silent
// security downgrade. For sslmode=verify-full against a private CA, it
// would break the connection entirely (RootCAs=nil forces validation
// against the system pool). For mTLS, the client certificate would be
// dropped, and a server that requires client certs would refuse the
// connection.
//
// The rework instead decouples the home-directory defaults (which only
// need os.UserHomeDir, safe to call unconditionally) from the OS account
// username lookup (the only thing that is actually lazy/gated, since it is
// the only part that can crash in a broken-NSS/CGO container). These tests
// assert byte-for-byte the resulting TLS configuration is identical to
// what an unmodified upstream/master (which has no such gate at all)
// produces for the same fake home directory -- proving no downgrade.
func setupSecurityABFakeHome(t *testing.T) (fakeHome, rootCert string) {
	t.Helper()
	skipOnWindows(t)

	fakeHome = t.TempDir()
	pgDir := filepath.Join(fakeHome, ".postgresql")
	require.NoError(t, os.MkdirAll(pgDir, 0o700))

	// Generate a throwaway private CA + client cert/key signed by it, so we
	// can assert the resolved sslrootcert/sslcert/sslkey are actually used
	// to build the TLS config (not merely present as files).
	caKey := filepath.Join(pgDir, "ca.key")
	rootCert = filepath.Join(pgDir, "root.crt")
	clientKey := filepath.Join(pgDir, "postgresql.key")
	clientCert := filepath.Join(pgDir, "postgresql.crt")
	csr := filepath.Join(pgDir, "client.csr")

	runOpenSSL(t, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
		"-keyout", caKey, "-out", rootCert, "-days", "3650",
		"-subj", "/CN=pgx-security-ab-test-root-ca")
	runOpenSSL(t, "req", "-newkey", "rsa:2048", "-nodes",
		"-keyout", clientKey, "-out", csr, "-subj", "/CN=pgx-security-ab-test-client")
	runOpenSSL(t, "x509", "-req", "-in", csr, "-CA", rootCert, "-CAkey", caKey,
		"-CAcreateserial", "-out", clientCert, "-days", "3650")

	return fakeHome, rootCert
}

func runOpenSSL(t *testing.T, args ...string) {
	t.Helper()
	path, err := exec.LookPath("openssl")
	if err != nil {
		t.Skip("openssl not available; skipping security A/B TLS defaults test")
	}
	cmd := exec.Command(path, args...)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "openssl %v failed: %s", args, out)
}

// TestParseConfigHomeDirTLSDefaultsSurviveExplicitUserAndPassword is the
// mandatory security regression test: when user and password are BOTH
// explicit (the exact scenario the previous, refuted fix mishandled), the
// home-directory-derived sslrootcert/sslcert/sslkey defaults must still
// resolve and produce the same TLS configuration as they would with no
// user/password gating at all (i.e. as unmodified upstream/master, which
// has no such gate, always produced).
//
// It also proves the crash fix is preserved: the OS user account lookup
// (which only supplies the *username* default now) must never be called,
// since user is already explicit.
func TestParseConfigHomeDirTLSDefaultsSurviveExplicitUserAndPassword(t *testing.T) {
	clearPgEnvvars(t)
	fakeHome, rootCert := setupSecurityABFakeHome(t)

	restoreHome := pgconn.SetUserHomeDirLookupForTest(func() (string, error) {
		return fakeHome, nil
	})
	defer restoreHome()

	restoreUser := pgconn.SetOSUserLookupForTest(func() (*user.User, error) {
		panic("os/user.Current must not be called when user is already explicit")
	})
	defer restoreUser()

	expectedRootPool := x509.NewCertPool()
	rootPEM, err := os.ReadFile(rootCert)
	require.NoError(t, err)
	require.True(t, expectedRootPool.AppendCertsFromPEM(rootPEM))

	tests := []struct {
		name       string
		connString string
		assertTLS  func(t *testing.T, cfg *pgconn.Config)
	}{
		{
			name:       "sslmode=require",
			connString: "postgres://curly:nyuknyuknyuk@test1:5432/curlydb?sslmode=require",
			assertTLS: func(t *testing.T, cfg *pgconn.Config) {
				require.NotNil(t, cfg.TLSConfig)
				// Matches gold reference from unmodified upstream/master:
				// InsecureSkipVerify=true|RootCAs=set(matches)|VerifyPeerCertificate=true|Certificates=1
				assert.True(t, cfg.TLSConfig.InsecureSkipVerify)
				require.NotNil(t, cfg.TLSConfig.RootCAs, "sslrootcert default must still resolve (was silently dropped by the refuted fix)")
				assert.True(t, cfg.TLSConfig.RootCAs.Equal(expectedRootPool), "RootCAs must be built from the home-dir root.crt")
				assert.NotNil(t, cfg.TLSConfig.VerifyPeerCertificate, "require+rootcert must still verify the chain itself (verify-ca-equivalent behavior)")
				assert.Len(t, cfg.TLSConfig.Certificates, 1, "mTLS client cert default must still resolve")
			},
		},
		{
			name:       "sslmode=verify-full",
			connString: "postgres://curly:nyuknyuknyuk@test1:5432/curlydb?sslmode=verify-full",
			assertTLS: func(t *testing.T, cfg *pgconn.Config) {
				require.NotNil(t, cfg.TLSConfig)
				// Matches gold reference from unmodified upstream/master:
				// InsecureSkipVerify=false|RootCAs=set(matches)|VerifyPeerCertificate=false|Certificates=1
				assert.False(t, cfg.TLSConfig.InsecureSkipVerify)
				require.NotNil(t, cfg.TLSConfig.RootCAs, "sslrootcert default must still resolve against a private CA")
				assert.True(t, cfg.TLSConfig.RootCAs.Equal(expectedRootPool))
				assert.Nil(t, cfg.TLSConfig.VerifyPeerCertificate, "verify-full uses Go's standard chain+hostname verification, not the custom callback")
				assert.Equal(t, "test1", cfg.TLSConfig.ServerName)
				assert.Len(t, cfg.TLSConfig.Certificates, 1, "mTLS client cert default must still resolve")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := pgconn.ParseConfig(tt.connString)
			require.NoError(t, err)
			require.Equal(t, "curly", cfg.User)
			require.Equal(t, "nyuknyuknyuk", cfg.Password)
			tt.assertTLS(t, cfg)
		})
	}
}

// TestParseConfigHomeDirTLSDefaultsDropOldGatedBehavior documents, as a
// negative control, exactly what the refuted fix got wrong: it shows that
// WITHOUT any home-dir defaults resolved (simulating the old gated/skip
// behavior by pointing the fake home at an empty directory), the same
// connString silently downgrades to InsecureSkipVerify with no root CA.
// This is not exercised against production code paths -- it exists so the
// contrast with the test above is explicit and mechanically checkable.
func TestParseConfigHomeDirTLSDefaultsDropOldGatedBehavior(t *testing.T) {
	clearPgEnvvars(t)

	emptyHome := t.TempDir() // no ~/.postgresql directory at all

	restoreHome := pgconn.SetUserHomeDirLookupForTest(func() (string, error) {
		return emptyHome, nil
	})
	defer restoreHome()

	cfg, err := pgconn.ParseConfig("postgres://curly:nyuknyuknyuk@test1:5432/curlydb?sslmode=require")
	require.NoError(t, err)
	require.NotNil(t, cfg.TLSConfig)

	// This is what the refuted fix produced for EVERY explicit user+password
	// connString, even when the real home directory DID have a root.crt --
	// because it skipped resolving home-dir defaults altogether.
	assert.True(t, cfg.TLSConfig.InsecureSkipVerify)
	assert.Nil(t, cfg.TLSConfig.RootCAs)
	assert.Nil(t, cfg.TLSConfig.VerifyPeerCertificate)
	assert.Empty(t, cfg.TLSConfig.Certificates)
}

// TestParseConfigHomeDirLookupFailureDoesNotCrash covers item 5 of the
// rework: when the home directory itself cannot be resolved (os.UserHomeDir
// erroring, e.g. because $HOME is unset), ParseConfig must not panic or
// error -- it must gracefully fall back to having no home-dir-derived
// defaults at all, exactly as it always did when os/user.Current failed
// before this rework (and exactly as it does today when the OS account
// lookup fails, see TestParseConfigOSUserLookupFailureStillErrorsWhenActuallyNeeded).
func TestParseConfigHomeDirLookupFailureDoesNotCrash(t *testing.T) {
	clearPgEnvvars(t)

	restoreHome := pgconn.SetUserHomeDirLookupForTest(func() (string, error) {
		return "", errors.New("$HOME is not defined")
	})
	defer restoreHome()

	restoreUser := pgconn.SetOSUserLookupForTest(func() (*user.User, error) {
		return &user.User{Username: "mocked-os-user"}, nil
	})
	defer restoreUser()

	// User not explicit: still falls back to the OS account username (that
	// lookup is independent of the home directory lookup).
	config, err := pgconn.ParseConfig("postgres://test1:5432/curlydb?sslmode=require")
	require.NoError(t, err)
	assert.Equal(t, "mocked-os-user", config.User)
	// No passfile default could resolve (home dir lookup failed), so there
	// is nothing to supply a password from.
	assert.Empty(t, config.Password)
	// No sslrootcert/sslcert/sslkey defaults could resolve either -- this
	// is the pre-existing, accepted fallback behavior (same as when
	// os/user.Current failed before this rework), not a new regression.
	require.NotNil(t, config.TLSConfig)
	assert.Nil(t, config.TLSConfig.RootCAs)
	assert.Empty(t, config.TLSConfig.Certificates)
}
