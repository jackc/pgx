package pgconn

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// currentOSUser resolves the current OS user account. It is a seam so tests
// can simulate os/user.Current failing -- including the unrecoverable
// crashes reported in some restricted/broken-NSS container environments --
// without needing such an environment.
var currentOSUser = user.Current

// userHomeDir resolves the current user's home directory. It is a seam so
// tests can simulate os.UserHomeDir failing without needing to unset the
// relevant environment variables for the whole process.
//
// os.UserHomeDir only reads %USERPROFILE% (falling back to
// %HOMEDRIVE%+%HOMEPATH%) on this platform; it never calls into the Windows
// user account APIs, so unlike os/user.Current it cannot crash in
// restricted/broken container environments. That also means it can resolve
// a different directory than the OS account API's reported home directory
// if those environment variables have been overridden -- this is an
// accepted tradeoff so that home-directory-derived defaults (pg_service.conf)
// keep resolving even when the OS user account lookup itself is unavailable
// or unsafe to call.
var userHomeDir = os.UserHomeDir

func defaultSettings() map[string]string {
	settings := make(map[string]string)

	settings["host"] = defaultHost()
	settings["port"] = "5432"
	settings["target_session_attrs"] = "any"

	// The %APPDATA%\postgresql-derived defaults (pgpass.conf and the client
	// SSL certificate/key/root files) only need the APPDATA environment
	// variable, not the full OS user account, so they are resolved
	// unconditionally here. This keeps them available even when looking up
	// the OS user account (needed only for the default PostgreSQL user
	// name, see osUserSettings) is slow, fails, or -- in some restricted
	// container environments with a broken NSS/CGO setup -- would crash the
	// process.
	if appData := os.Getenv("APPDATA"); appData != "" {
		settings["passfile"] = filepath.Join(appData, "postgresql", "pgpass.conf")
		sslcert := filepath.Join(appData, "postgresql", "postgresql.crt")
		sslkey := filepath.Join(appData, "postgresql", "postgresql.key")
		if _, err := os.Stat(sslcert); err == nil {
			if _, err := os.Stat(sslkey); err == nil {
				// Both the cert and key must be present to use them, or do not use either
				settings["sslcert"] = sslcert
				settings["sslkey"] = sslkey
			}
		}
		sslrootcert := filepath.Join(appData, "postgresql", "root.crt")
		if _, err := os.Stat(sslrootcert); err == nil {
			settings["sslrootcert"] = sslrootcert
		}
	}

	// The default ~/.pg_service.conf location is derived from the user's
	// home directory, resolved the same crash-safe way as on other
	// platforms (see defaults.go).
	if homeDir, err := userHomeDir(); err == nil {
		settings["servicefile"] = filepath.Join(homeDir, ".pg_service.conf")
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
		// Windows gives us the username here as `DOMAIN\user` or `LOCALPCNAME\user`,
		// but the libpq default is just the `user` portion, so we strip off the first part.
		username := user.Username
		if strings.Contains(username, "\\") {
			username = username[strings.LastIndex(username, "\\")+1:]
		}

		settings["user"] = username
	}

	return settings
}

// defaultHost attempts to mimic libpq's default host. libpq uses the default unix socket location on *nix and localhost
// on Windows. The default socket location is compiled into libpq. Since pgx does not have access to that default it
// checks the existence of common locations.
func defaultHost() string {
	return "localhost"
}
