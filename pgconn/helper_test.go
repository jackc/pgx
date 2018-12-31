package pgconn_test

import (
	"testing"

	"github.com/jackc/pgx/pgconn"

	"github.com/stretchr/testify/require"
)

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	require.Nil(t, conn.Close())
}
