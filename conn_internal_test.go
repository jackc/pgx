package pgx

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseConfig(t testing.TB, connString string) *ConnConfig {
	config, err := ParseConfig(connString)
	require.Nil(t, err)
	return config
}

func mustConnect(t testing.TB, config *ConnConfig) *Conn {
	conn, err := ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("Unable to establish connection: %v", err)
	}
	return conn
}

// Ensures the connection limits the size of its cached objects.
// This test examines the internals of *Conn so must be in the same package.
func TestStmtCacheSizeLimit(t *testing.T) {
	const cacheLimit = 16

	connConfig := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	connConfig.StatementCacheCapacity = cacheLimit
	conn := mustConnect(t, connConfig)
	defer func() {
		err := conn.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}()

	// run a set of unique queries that should overflow the cache
	ctx := context.Background()
	for i := 0; i < cacheLimit*2; i++ {
		uniqueString := fmt.Sprintf("unique %d", i)
		uniqueSQL := fmt.Sprintf("select '%s'", uniqueString)
		var output string
		err := conn.QueryRow(ctx, uniqueSQL).Scan(&output)
		require.NoError(t, err)
		require.Equal(t, uniqueString, output)
	}
	// preparedStatements contains cacheLimit+1 because deallocation happens before the query
	assert.Len(t, conn.preparedStatements, cacheLimit+1)
	assert.Equal(t, cacheLimit, conn.statementCache.Len())
}

func TestPrepareThreshold_Internal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Test with prepare_threshold=0 (default)
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.PrepareThreshold = 0
	conn := mustConnect(t, config)
	defer func() { _ = conn.Close(ctx) }()

	for i := 0; i < 5; i++ {
		rows, err := conn.Query(ctx, "select $1::text", fmt.Sprintf("test%d", i))
		require.NoError(t, err)
		rows.Close()
	}
	// Should prepare immediately
	assert.NotNil(t, conn.statementCache)
	assert.Greater(t, conn.statementCache.Len(), 0)

	// Test with prepare_threshold=3
	config = mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.PrepareThreshold = 3
	conn = mustConnect(t, config)
	defer func() { _ = conn.Close(ctx) }()

	for i := 0; i < 2; i++ {
		rows, err := conn.Query(ctx, "select $1::text", fmt.Sprintf("test%d", i))
		require.NoError(t, err)
		rows.Close()
	}
	assert.NotNil(t, conn.statementCache)
	assert.Equal(t, 0, conn.statementCache.Len())

	for i := 2; i < 4; i++ {
		rows, err := conn.Query(ctx, "select $1::text", fmt.Sprintf("test%d", i))
		require.NoError(t, err)
		rows.Close()
	}
	assert.NotNil(t, conn.statementCache)
	assert.Greater(t, conn.statementCache.Len(), 0)

	// Test with prepare_threshold=1
	config = mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.PrepareThreshold = 1
	conn = mustConnect(t, config)
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, "select $1::text", "test")
	require.NoError(t, err)
	rows.Close()
	assert.NotNil(t, conn.statementCache)
	assert.Greater(t, conn.statementCache.Len(), 0)
}

func TestPrepareThresholdWithDifferentQueries_Internal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.PrepareThreshold = 2
	conn := mustConnect(t, config)
	defer func() { _ = conn.Close(ctx) }()

	queries := []string{
		"select $1::text",
		"select $1::int",
		"select $1::float",
	}
	for _, query := range queries {
		rows, err := conn.Query(ctx, query, "test")
		require.NoError(t, err)
		rows.Close()
	}
	assert.Equal(t, 0, conn.statementCache.Len())
	for _, query := range queries {
		rows, err := conn.Query(ctx, query, "test")
		require.NoError(t, err)
		rows.Close()
	}
	assert.Equal(t, len(queries), conn.statementCache.Len())
}

func TestPrepareThresholdWithTransaction_Internal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.PrepareThreshold = 2
	conn := mustConnect(t, config)
	defer func() { _ = conn.Close(ctx) }()
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "select $1::text", "test")
	require.NoError(t, err)
	rows.Close()
	assert.Equal(t, 0, conn.statementCache.Len())
	rows, err = tx.Query(ctx, "select $1::text", "test")
	require.NoError(t, err)
	rows.Close()
	assert.Equal(t, 1, conn.statementCache.Len())
	err = tx.Commit(ctx)
	require.NoError(t, err)
	rows, err = conn.Query(ctx, "select $1::text", "test")
	require.NoError(t, err)
	rows.Close()
	assert.Equal(t, 1, conn.statementCache.Len())
}
