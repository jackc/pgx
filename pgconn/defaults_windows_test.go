package pgconn_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupWindowsServiceFileDirs points APPDATA and the home directory lookup at
// fresh temporary directories and returns them.
func setupWindowsServiceFileDirs(t *testing.T) (appData, homeDir string) {
	t.Helper()
	clearPgEnvvars(t)
	t.Setenv("PGSERVICE", "")
	t.Setenv("PGSERVICEFILE", "")

	appData = t.TempDir()
	t.Setenv("APPDATA", appData)

	homeDir = t.TempDir()
	restore := pgconn.SetUserHomeDirLookupForTest(func() (string, error) { return homeDir, nil })
	t.Cleanup(restore)

	return appData, homeDir
}

func writeServiceFile(t *testing.T, path, host string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("[abc]\nhost="+host+"\n"), 0o600))
}

// libpq looks for the per-user service file at
// %APPDATA%\postgresql\.pg_service.conf on Windows.
func TestParseConfigReadsDefaultServiceFileFromAppDataOnWindows(t *testing.T) {
	appData, _ := setupWindowsServiceFileDirs(t)
	writeServiceFile(t, filepath.Join(appData, "postgresql", ".pg_service.conf"), "appdata.example.com")

	config, err := pgconn.ParseConfig("service=abc")
	require.NoError(t, err)
	assert.Equal(t, "appdata.example.com", config.Host)
}

func TestParseConfigPrefersAppDataServiceFileOnWindows(t *testing.T) {
	appData, homeDir := setupWindowsServiceFileDirs(t)
	writeServiceFile(t, filepath.Join(appData, "postgresql", ".pg_service.conf"), "appdata.example.com")
	writeServiceFile(t, filepath.Join(homeDir, ".pg_service.conf"), "home.example.com")

	config, err := pgconn.ParseConfig("service=abc")
	require.NoError(t, err)
	assert.Equal(t, "appdata.example.com", config.Host)
}

// Earlier versions of pgx looked for the service file in the home directory,
// so it is still used when there is none in %APPDATA%\postgresql.
func TestParseConfigFallsBackToHomeDirServiceFileOnWindows(t *testing.T) {
	_, homeDir := setupWindowsServiceFileDirs(t)
	writeServiceFile(t, filepath.Join(homeDir, ".pg_service.conf"), "home.example.com")

	config, err := pgconn.ParseConfig("service=abc")
	require.NoError(t, err)
	assert.Equal(t, "home.example.com", config.Host)
}
