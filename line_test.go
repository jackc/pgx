package pgtype_test

import (
	"testing"

	version "github.com/hashicorp/go-version"
	"github.com/jackc/pgx/pgtype"
)

func TestLineTranscode(t *testing.T) {
	conn := mustConnectPgx(t)
	serverVersion, err := version.NewVersion(conn.RuntimeParams["server_version"])
	if err != nil {
		t.Fatalf("cannot get server version: %v", err)
	}
	mustClose(t, conn)

	minVersion := version.Must(version.NewVersion("9.4"))

	if serverVersion.LessThan(minVersion) {
		t.Skipf("Skipping line test for server version %v", serverVersion)
	}

	testSuccessfulTranscode(t, "line", []interface{}{
		&pgtype.Line{
			A: 1.23, B: 4.56, C: 7.89,
			Status: pgtype.Present,
		},
		&pgtype.Line{
			A: -1.23, B: -4.56, C: -7.89,
			Status: pgtype.Present,
		},
		&pgtype.Line{Status: pgtype.Null},
	})
}
