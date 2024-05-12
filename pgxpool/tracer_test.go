package pgxpool_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type testTracer struct {
	traceAcquireStart func(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context
	traceAcquireEnd   func(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData)
	traceRelease      func(pool *pgxpool.Pool, data pgxpool.TraceReleaseData)
}

type ctxKey string

func (tt *testTracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	if tt.traceAcquireStart != nil {
		return tt.traceAcquireStart(ctx, pool, data)
	}
	return ctx
}

func (tt *testTracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	if tt.traceAcquireEnd != nil {
		tt.traceAcquireEnd(ctx, pool, data)
	}
}

func (tt *testTracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	if tt.traceRelease != nil {
		tt.traceRelease(pool, data)
	}
}

func (tt *testTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
}

func TestTraceAcquire(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnConfig.Tracer = tracer

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	traceAcquireStartCalled := false
	tracer.traceAcquireStart = func(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
		traceAcquireStartCalled = true
		require.NotNil(t, pool)
		return context.WithValue(ctx, ctxKey("fromTraceAcquireStart"), "foo")
	}

	traceAcquireEndCalled := false
	tracer.traceAcquireEnd = func(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
		traceAcquireEndCalled = true
		require.Equal(t, "foo", ctx.Value(ctxKey("fromTraceAcquireStart")))
		require.NotNil(t, pool)
		require.NotNil(t, data.Conn)
		require.NoError(t, data.Err)
	}

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer c.Release()
	require.True(t, traceAcquireStartCalled)
	require.True(t, traceAcquireEndCalled)

	traceAcquireStartCalled = false
	traceAcquireEndCalled = false
	tracer.traceAcquireEnd = func(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
		traceAcquireEndCalled = true
		require.NotNil(t, pool)
		require.Nil(t, data.Conn)
		require.Error(t, data.Err)
	}

	ctx, cancel = context.WithCancel(ctx)
	cancel()
	_, err = pool.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, traceAcquireStartCalled)
	require.True(t, traceAcquireEndCalled)
}

func TestTraceRelease(t *testing.T) {
	t.Parallel()

	tracer := &testTracer{}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnConfig.Tracer = tracer

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	traceReleaseCalled := false
	tracer.traceRelease = func(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
		traceReleaseCalled = true
		require.NotNil(t, pool)
		require.NotNil(t, data.Conn)
	}

	c, err := pool.Acquire(ctx)
	require.NoError(t, err)
	c.Release()
	require.True(t, traceReleaseCalled)
}
