//go:build !windows
// +build !windows

package pgconn

import (
	"os"
	"os/user"
	"path/filepath"
)

// currentOSUser resolves the current OS user account. It is a seam so tests
// can simulate os/user.Current failing -- including the unrecoverable
// crashes reported in some restricted/broken-NSS container environments --
// without needing such an environment.
var currentOSUser = user.Current

// userHomeDir resolves the current user's home directory. It is a seam so
// tests can simulate os.UserHomeDir failing without needing to unset $HOME
// for the whole process.
//
// os.UserHomeDir only reads the $HOME environment variable on this platform;
// it never calls into cgo or NSS, so unlike os/user.Current it cannot crash
// in restricted/broken container environments. That also means it can
// resolve a different directory than the OS account's passwd-file home
// directory if $HOME has been overridden -- this is an accepted tradeoff so
// that home-directory-derived defaults (pgpass, pg_service.conf, client SSL
// certificate/key/root files) keep resolving even when the OS user account
// lookup itself is unavailable or unsafe to call.
var userHomeDir = os.UserHomeDir

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = defaultHost()
	settings["port"] = "5432"
	settings["target_session_attrs"] = "any"

	// The home-directory-derived defaults (~/.pgpass, ~/.pg_service.conf,
	// and the client SSL certificate/key/root files under ~/.postgresql)
	// only need the user's home directory, not the full OS user account, so
	// they are resolved unconditionally here via os.UserHomeDir. This keeps
	// them available even when looking up the OS user account (needed only
	// for the default PostgreSQL user name, see osUserSettings) is slow,
	// fails, or -- in some restricted container environments with a broken
	// NSS/CGO setup -- would crash the process.
	if homeDir, err := userHomeDir(); err == nil {
		settings["passfile"] = filepath.Join(homeDir, ".pgpass")
		settings["servicefile"] = filepath.Join(homeDir, ".pg_service.conf")
		sslcert := filepath.Join(homeDir, ".postgresql", "postgresql.crt")
		sslkey := filepath.Join(homeDir, ".postgresql", "postgresql.key")
		if _, err := os.Stat(sslcert); err == nil {
			if _, err := os.Stat(sslkey); err == nil {
				// Both the cert and key must be present to use them, or do not use either
				settings["sslcert"] = sslcert
				settings["sslkey"] = sslkey
			}
		}
		sslrootcert := filepath.Join(homeDir, ".postgresql", "root.crt")
		if _, err := os.Stat(sslrootcert); err == nil {
			settings["sslrootcert"] = sslrootcert
		}
	}

	return settings
}

// osUserSettings returns the default PostgreSQL user name derived from the
// current OS user account.
//
// Resolving this requires looking up the OS user account, which can be slow
// or, in some restricted container environments (e.g. distroless images
// with a broken NSS/CGO setup), crash the process. Callers must only call
// osUserSettings when the default user name is not already supplied by the
// connection string, environment, or service file.
func osUserSettings() map[string]string {
	settings := make(map[string]string)

	// Default to the OS user name. Purposely ignoring err getting user name from
	// OS. The client application will simply have to specify the user in that
	// case (which they typically will be doing anyway).
	user, err := currentOSUser()
	if err == nil {
		settings["user"] = user.Username
	}

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
