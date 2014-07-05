package pgx_test

import (
	"github.com/jackc/pgx"
	"testing"
)

func BenchmarkConnPool(b *testing.B) {
	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 5}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		b.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var conn *pgx.Conn
		if conn, err = pool.Acquire(); err != nil {
			b.Fatalf("Unable to acquire connection: %v", err)
		}
		pool.Release(conn)
	}
}
