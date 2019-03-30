package pgconn_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Nil(t, conn.Close(ctx))
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, pgConn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result := pgConn.ExecParams(ctx, "select generate_series(1,$1)", [][]byte{[]byte("3")}, nil, nil, nil).Read()
	cancel()

	require.Nil(t, result.Err)
	assert.Equal(t, 3, len(result.Rows))
	assert.Equal(t, "1", string(result.Rows[0][0]))
	assert.Equal(t, "2", string(result.Rows[1][0]))
	assert.Equal(t, "3", string(result.Rows[2][0]))
}
