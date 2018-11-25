package stdlib_test

import (
	"os"
)

var testConnStr = "postgres://pgx_md5:secret@127.0.0.1:5432/pgx_test"

func init() {
	s := os.Getenv("PGX_TEST_DATABASE")
	if len(s) > 0 {
		testConnStr = s
	}
}
