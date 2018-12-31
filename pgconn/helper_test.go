package pgconn_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/pgconn"

	"github.com/stretchr/testify/require"
)

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Nil(t, conn.Close(ctx))
}
