package pgxpool_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func BenchmarkAcquireAndRelease(b *testing.B) {
	pool, err := pgxpool.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(b, err)
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := pool.Acquire(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		c.Release()
	}
}

func BenchmarkMinimalPreparedSelectBaseline(b *testing.B) {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(b, err)

	config.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Prepare(ctx, "ps1", "select $1::int8")
		return err
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(b, err)

	conn, err := db.Acquire(context.Background())
	require.NoError(b, err)
	defer conn.Release()

	var n int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = conn.QueryRow(context.Background(), "ps1", i).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}

		if n != int64(i) {
			b.Fatalf("expected %d, got %d", i, n)
		}
	}
}

func BenchmarkMinimalPreparedSelect(b *testing.B) {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(b, err)

	config.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Prepare(ctx, "ps1", "select $1::int8")
		return err
	}

	db, err := pgxpool.ConnectConfig(context.Background(), config)
	require.NoError(b, err)

	var n int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = db.QueryRow(context.Background(), "ps1", i).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}

		if n != int64(i) {
			b.Fatalf("expected %d, got %d", i, n)
		}
	}
}
