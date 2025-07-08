//go:build !windows
// +build !windows

package pgconn

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
)

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = defaultHost()
	settings["port"] = "5432"

	// Default to the OS user name. Purposely ignoring err getting user name from
	// OS. The client application will simply have to specify the user in that
	// case (which they typically will be doing anyway).
	username := os.Getenv("USER") // Unix-like
	if username == "" {
		username = os.Getenv("USERNAME") // Windows
	}
	if username != "" {
		settings["user"] = username
	}

	homeDir, err := homedir.Dir()
	if err == nil {
		settings["passfile"] = filepath.Join(homeDir, ".pgpass")
		settings["servicefile"] = filepath.Join(homeDir, ".pg_service.conf")

		sslcert := filepath.Join(homeDir, ".postgresql", "postgresql.crt")
		sslkey := filepath.Join(homeDir, ".postgresql", "postgresql.key")
		if _, err := os.Stat(sslcert); err == nil {
			if _, err := os.Stat(sslkey); err == nil {
				settings["sslcert"] = sslcert
				settings["sslkey"] = sslkey
			}
		}

		sslrootcert := filepath.Join(homeDir, ".postgresql", "root.crt")
		if _, err := os.Stat(sslrootcert); err == nil {
			settings["sslrootcert"] = sslrootcert
		}
	}

	settings["target_session_attrs"] = "any"

	return settings
}

// defaultHost attempts to mimic libpq's default host. libpq uses the default unix socket location on *nix and localhost
// on Windows. The default socket location is compiled into libpq. Since pgx does not have access to that default it
// checks the existence of common locations.
func defaultHost() string {
	candidatePaths := []string{
		"/var/run/postgresql", // Debian
		"/private/tmp",        // OSX - homebrew
		"/tmp",                // standard PostgreSQL
	}

	for _, path := range candidatePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return "localhost"
}
