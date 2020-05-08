package pgx_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func testWithAndWithoutPreferSimpleProtocol(t *testing.T, f func(t *testing.T, conn *pgx.Conn)) {
	t.Run("SimpleProto",
		func(t *testing.T) {
			config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
			require.NoError(t, err)

			config.PreferSimpleProtocol = true
			conn, err := pgx.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			defer func() {
				err := conn.Close(context.Background())
				require.NoError(t, err)
			}()

			f(t, conn)

			ensureConnValid(t, conn)
		},
	)

	t.Run("DefaultProto",
		func(t *testing.T) {
			config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
			require.NoError(t, err)

			conn, err := pgx.ConnectConfig(context.Background(), config)
			require.NoError(t, err)
			defer func() {
				err := conn.Close(context.Background())
				require.NoError(t, err)
			}()

			f(t, conn)

			ensureConnValid(t, conn)
		},
	)
}

func mustConnectString(t testing.TB, connString string) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return conn
}

func mustParseConfig(t testing.TB, connString string) *pgx.ConnConfig {
	config, err := pgx.ParseConfig(connString)
	require.Nil(t, err)
	return config
}

func mustConnect(t testing.TB, config *pgx.ConnConfig) *pgx.Conn {
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return conn
}

func closeConn(t testing.TB, conn *pgx.Conn) {
	err := conn.Close(context.Background())
	if err != nil {
		t.Fatalf("conn.Close unexpectedly failed: %v", err)
	}
}

func mustExec(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag) {
	var err error
	if commandTag, err = conn.Exec(context.Background(), sql, arguments...); err != nil {
		t.Fatalf("Exec unexpectedly failed with %v: %v", sql, err)
	}
	return
}

// Do a simple query to ensure the connection is still usable
func ensureConnValid(t *testing.T, conn *pgx.Conn) {
	var sum, rowCount int32

	rows, err := conn.Query(context.Background(), "select generate_series(1,$1)", 10)
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	if rows.Err() != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}
