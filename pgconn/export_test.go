// File export_test exports some methods for better testing.

package pgconn

func NewParseConfigError(conn, msg string, err error) error {
	return &parseConfigError{
		connString: conn,
		msg:        msg,
		err:        err,
	}
}
