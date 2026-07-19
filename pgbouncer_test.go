package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestPgbouncerQueryExecModes(t *testing.T) {
	connString := os.Getenv("PGX_TEST_PGBOUNCER_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping due to missing environment variable %v", "PGX_TEST_PGBOUNCER_CONN_STRING")
	}

	tests := []struct {
		name                     string
		mode                     pgx.QueryExecMode
		statementCacheCapacity   int
		descriptionCacheCapacity int
	}{
		{
			name:                   "cache statement",
			mode:                   pgx.QueryExecModeCacheStatement,
			statementCacheCapacity: 32,
		},
		{
			name:                     "cache describe",
			mode:                     pgx.QueryExecModeCacheDescribe,
			descriptionCacheCapacity: 32,
		},
		{
			name: "exec",
			mode: pgx.QueryExecModeExec,
		},
		{
			name: "simple protocol",
			mode: pgx.QueryExecModeSimpleProtocol,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := mustParseConfig(t, connString)
			config.DefaultQueryExecMode = tt.mode
			config.StatementCacheCapacity = tt.statementCacheCapacity
			config.DescriptionCacheCapacity = tt.descriptionCacheCapacity

			testPgbouncer(t, config, 10, 100)
		})
	}
}

func testPgbouncer(t *testing.T, config *pgx.ConnConfig, workers, iterations int) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)

	for range workers {
		eg.Go(func() (err error) {
			conn, err := pgx.ConnectConfig(ctx, config)
			if err != nil {
				return err
			}
			defer func() {
				err = errors.Join(err, conn.Close(ctx))
			}()

			return exercisePgbouncerConn(ctx, conn, iterations)
		})
	}

	require.NoError(t, eg.Wait())
}

func exercisePgbouncerConn(ctx context.Context, conn *pgx.Conn, iterations int) error {
	for i := range iterations {
		var i32 int32
		var i64 int64
		var f32 float32
		var s string

		err := conn.QueryRow(ctx, "select $1::int4, $2::int8, $3::float4, $4::text", int32(i), int64(i+1), float32(i+2), "hi").Scan(&i32, &i64, &f32, &s)
		if err != nil {
			return err
		}
		if i32 != int32(i) || i64 != int64(i+1) || f32 != float32(i+2) || s != "hi" {
			return fmt.Errorf("unexpected query result: %d, %d, %f, %q", i32, i64, f32, s)
		}
	}

	commandTag, err := conn.Exec(ctx, "select $1::int4", int32(42))
	if err != nil {
		return err
	}
	if commandTag.String() != "SELECT 1" {
		return fmt.Errorf("unexpected command tag: %s", commandTag)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	var txValue int32
	if err := tx.QueryRow(ctx, "select $1::int4", int32(43)).Scan(&txValue); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if txValue != 43 {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("unexpected transaction query result: %d", txValue)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	batch := &pgx.Batch{}
	batch.Queue("select $1::int4", int32(44))
	batch.Queue("select $1::text", "batch")
	batchResults := conn.SendBatch(ctx, batch)

	var batchInt int32
	if err := batchResults.QueryRow().Scan(&batchInt); err != nil {
		_ = batchResults.Close()
		return err
	}
	var batchText string
	if err := batchResults.QueryRow().Scan(&batchText); err != nil {
		_ = batchResults.Close()
		return err
	}
	if err := batchResults.Close(); err != nil {
		return err
	}
	if batchInt != 44 || batchText != "batch" {
		return fmt.Errorf("unexpected batch query results: %d, %q", batchInt, batchText)
	}

	return nil
}
