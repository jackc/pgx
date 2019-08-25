package pgx_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgconn/stmtcache"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPgbouncerStatementCacheDescribe(t *testing.T) {
	connString := os.Getenv("PGX_TEST_PGBOUNCER_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_PGBOUNCER_CONN_STRING")
	}

	config := mustParseConfig(t, connString)
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModeDescribe, 1024)
	}

	testPgbouncer(t, config, 10, 100)
}

func TestPgbouncerSimpleProtocol(t *testing.T) {
	connString := os.Getenv("PGX_TEST_PGBOUNCER_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_PGBOUNCER_CONN_STRING")
	}

	config := mustParseConfig(t, connString)
	config.BuildStatementCache = nil
	config.PreferSimpleProtocol = true

	testPgbouncer(t, config, 10, 100)
}

func testPgbouncer(t *testing.T, config *pgx.ConnConfig, workers, iterations int) {
	doneChan := make(chan struct{})

	for i := 0; i < workers; i++ {
		go func() {
			defer func() { doneChan <- struct{}{} }()
			conn, err := pgx.ConnectConfig(context.Background(), config)
			require.Nil(t, err)
			defer closeConn(t, conn)

			for i := 0; i < iterations; i++ {
				var i32 int32
				var i64 int64
				var f32 float32
				var s string
				var s2 string
				err = conn.QueryRow(context.Background(), "select 1::int4, 2::int8, 3::float4, 'hi'::text").Scan(&i32, &i64, &f32, &s)
				require.NoError(t, err)
				assert.Equal(t, int32(1), i32)
				assert.Equal(t, int64(2), i64)
				assert.Equal(t, float32(3), f32)
				assert.Equal(t, "hi", s)

				err = conn.QueryRow(context.Background(), "select 1::int8, 2::float4, 'bye'::text, 4::int4, 'whatever'::text").Scan(&i64, &f32, &s, &i32, &s2)
				require.NoError(t, err)
				assert.Equal(t, int64(1), i64)
				assert.Equal(t, float32(2), f32)
				assert.Equal(t, "bye", s)
				assert.Equal(t, int32(4), i32)
				assert.Equal(t, "whatever", s2)
			}
		}()
	}

	for i := 0; i < workers; i++ {
		<-doneChan
	}

}
