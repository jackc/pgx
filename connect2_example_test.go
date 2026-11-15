package pgx_test

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
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
	pooledDB := stdlib.OpenDBFromPool(pool)
	defer pooledDB.Close()
}

func ExampleConn_Config_connect2() {
	ctx := context.Background()
	conn, err := pgx.Connect2(ctx, os.Getenv("DATABASE_URL"), pgx.ConnectOptions2{})
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	config := conn.Config()
	// Repeat input resolution with the captured arguments. The snapshot cannot
	// be passed to ConnectConfig.
	replacement, err := pgx.Connect2(ctx, config.ConnString(), *config.Connect2Options)
	if err != nil {
		return
	}
	defer replacement.Close(ctx)
}
