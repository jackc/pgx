// File export_test exports some methods for better testing.

package pgconn

import "os/user"

// SetOSUserLookupForTest overrides the OS user account lookup used to
// resolve the default PostgreSQL user name. It lets tests simulate
// os/user.Current failing -- including the unrecoverable crashes reported
// in some restricted/broken-NSS container environments -- without needing
// such an environment. It returns a function that restores the previous
// lookup.
func SetOSUserLookupForTest(lookup func() (*user.User, error)) (restore func()) {
	prev := currentOSUser
	currentOSUser = lookup
	return func() { currentOSUser = prev }
}

// SetUserHomeDirLookupForTest overrides the home directory lookup used to
// resolve the home-directory-derived connection defaults (passfile,
// servicefile, sslcert, sslkey, sslrootcert). It lets tests simulate
// os.UserHomeDir failing (e.g. $HOME unset) without needing to unset the
// relevant environment variable for the whole process. It returns a
// function that restores the previous lookup.
func SetUserHomeDirLookupForTest(lookup func() (string, error)) (restore func()) {
	prev := userHomeDir
	userHomeDir = lookup
	return func() { userHomeDir = prev }
}
