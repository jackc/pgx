package pgx_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestSimpleProtocolUnquotedDollarIdentifier(t *testing.T) {
	ctx := t.Context()
	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)
	for _, name := range []string{"value$1", "_value$2", "café$1", "value$tag$"} {
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
			t.Run(name+"/"+mode.String(), func(t *testing.T) {
				var result int
				sql := "select " + name + " from (values(42)) as data(" + name + ") where " + name + "=$1"
				err := conn.QueryRow(ctx, sql, mode, 42).Scan(&result)
				require.NoError(t, err)
				require.Equal(t, 42, result)
			})
		}
	}
}
