package pgxpool_test

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestAcquireContextCancelRace(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_DATABASE")
	if connString == "" {
		t.Skip("PGX_TEST_DATABASE is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(connString)
	require.NoError(t, err)
	config.MaxConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			timeout := time.Duration(rand.Intn(5)+1) * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			if conn, err := pool.Acquire(ctx); err == nil {
				time.Sleep(2 * time.Millisecond)
				conn.Release()
			}
		}()
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	require.Equal(t, int32(0), pool.Stat().AcquiredConns())
}
