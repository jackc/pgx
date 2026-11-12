package pgx_test

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func ExampleConnect2() {
	ctx := context.Background()
	conn, err := pgx.Connect2(ctx, os.Getenv("DATABASE_URL"), pgx.ConnectOptions2{})
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	var greeting string
	_ = conn.QueryRow(ctx, "select 'hello'").Scan(&greeting)
}

func ExampleConnectFunc() {
	ctx := context.Background()
	// A third-party factory uses the same public physical entry point as built-in
	// adapters. Every invocation re-resolves external inputs.
	input := os.Getenv("DATABASE_URL")
	physical := pgconn.ConnectOptions2{}.Copy()
	factory := func(ctx context.Context) (*pgconn.PgConn, error) { return pgconn.Connect2(ctx, input, physical) }
	conn, err := pgx.ConnectFunc(ctx, factory, pgx.ConnOptions{})
	if err != nil {
		return
	}
	defer conn.Close(ctx)
}

func ExampleConnect2_adapters() {
	ctx := context.Background()
	input := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New2(ctx, input, pgxpool.Options{})
	if err != nil {
		return
	}
	defer pool.Close()
	db := stdlib.OpenDB2(input, stdlib.Options2{})
	defer db.Close()
	pooledDB := stdlib.OpenDBFromPool2(pool)
	defer pooledDB.Close()
}
