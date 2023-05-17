package pgx

import (
	"context"
	"fmt"
	"os"
	"testing"

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
