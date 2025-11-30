//go:build !windows
// +build !windows

package pgconn

import (
	"os"
	"os/user"
	"path/filepath"
)

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = defaultHost()
	settings["port"] = "5432"

	// Default to the OS user name. Purposely ignoring err getting user name from
	// OS. The client application will simply have to specify the user in that
	// case (which they typically will be doing anyway).
	userVar, err := user.Current()
	if err == nil {
		settings["user"] = userVar.Username
		settings["passfile"] = filepath.Join(userVar.HomeDir, ".pgpass")
		settings["servicefile"] = filepath.Join(userVar.HomeDir, ".pg_service.conf")
		sslcert := filepath.Join(userVar.HomeDir, ".postgresql", "postgresql.crt")
		sslkey := filepath.Join(userVar.HomeDir, ".postgresql", "postgresql.key")
		if _, err := os.Stat(sslcert); err == nil {
			if _, err := os.Stat(sslkey); err == nil {
				// Both the cert and key must be present to use them, or do not use either
				settings["sslcert"] = sslcert
				settings["sslkey"] = sslkey
			}
		}
		sslrootcert := filepath.Join(userVar.HomeDir, ".postgresql", "root.crt")
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

	for i := range candidatePaths {
		if _, err := os.Stat(candidatePaths[i]); err == nil {
			return candidatePaths[i]
		}
	}

	return "localhost"
}
