package pgx_test

import (
	"context"
	"github.com/stretchr/testify/assert"
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

func assertConfigsEqual(t *testing.T, expected, actual *pgx.ConnConfig, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.Logger, actual.Logger, "%s - Logger", testName)
	assert.Equalf(t, expected.LogLevel, actual.LogLevel, "%s - LogLevel", testName)
	assert.Equalf(t, expected.ConnString(), actual.ConnString(), "%s - ConnString", testName)
	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.BuildStatementCache == nil, actual.BuildStatementCache == nil, "%s - BuildStatementCache", testName)
	assert.Equalf(t, expected.PreferSimpleProtocol, actual.PreferSimpleProtocol, "%s - PreferSimpleProtocol", testName)

	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify, "%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t, expected.TLSConfig.ServerName, actual.TLSConfig.ServerName, "%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t, expected.Fallbacks[i].TLSConfig == nil, actual.Fallbacks[i].TLSConfig == nil, "%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.InsecureSkipVerify, actual.Fallbacks[i].TLSConfig.InsecureSkipVerify, "%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.ServerName, actual.Fallbacks[i].TLSConfig.ServerName, "%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}
